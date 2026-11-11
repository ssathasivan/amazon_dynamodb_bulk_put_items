# Amazon DynamoDB Bulk Put Items - A Developer's Take

Loading a large batch of data into a DynamoDB table that is already serving traffic has always needed care. Until now, it meant writing a `BatchWriteItem` loader, or running a Glue or EMR job, and then handling capacity and throttling yourself. Import from S3 doesn't help here, because it only creates new tables ([AWS Database Blog](https://aws.amazon.com/blogs/database/amazon-dynamodb-can-now-import-amazon-s3-data-into-a-new-table/)).

On November 5, 2026, AWS announced [bulk put items for Amazon DynamoDB](https://aws.amazon.com/about-aws/whats-new/2026/11/amazon-dynamodb-now-supports-bulk-put-items/). You point it at files in S3, and DynamoDB reads them, writes the items into an existing table, and reports what it could not write. This article explains how it works, and shares what we found when we tested it against `BatchWriteItem` with 1 million and 10 million items, with and without app traffic on the table.

The complete source, a Python app and a CDK stack, is available in the [amazon_dynamodb_bulk_put_items](https://github.com/ssathasivan/amazon_dynamodb_bulk_put_items) repository.

## How it works

Bulk put items is an asynchronous operation. `StartBulkPutItems` returns an operation ARN straight away, and DynamoDB reads, checks and writes the records in the background. The writes don't use the table's capacity, so they don't compete with your application. The third test checks this with app traffic running during a load.

The input is line-delimited DynamoDB JSON or [Amazon Ion](https://amazon-ion.github.io/ion-docs/), either uncompressed or compressed with gzip or zstd. Each line is one record: the item to write and, optionally, a condition. All files under one S3 prefix must use the same format and compression. The files can also be in a different account from the table.

Every operation runs on one of two performance tiers. Standard is the default, for loads without a deadline, and is in all commercial Regions and AWS GovCloud (US). Accelerated is for loads that must finish fast, is in a subset of Regions, and costs twice as much.

## Conditions

A record's condition decides whether its item is written. It uses `PutItem` syntax and is checked against the item already in the table. These three records come from our second test, where the table already holds all three products at `Version 1` (`payload` is shortened):

```json
{"Item": {"sku": {"S": "sku-000000005"}, "category": {"S": "cat-05"}, "Version": {"N": "2"}, "title": {"S": "Product 5"}, "payload": {"S": "xxx..."}}}
{"Item": {"sku": {"S": "sku-000020005"}, "category": {"S": "cat-05"}, "Version": {"N": "2"}, "title": {"S": "Product 20005"}, "payload": {"S": "xxx..."}}, "ConditionExpression": "attribute_not_exists(sku)"}
{"Item": {"sku": {"S": "sku-000040005"}, "category": {"S": "cat-05"}, "Version": {"N": "2"}, "title": {"S": "Product 40005"}, "payload": {"S": "xxx..."}}, "ConditionExpression": "#v < :v", "ExpressionAttributeNames": {"#v": "Version"}, "ExpressionAttributeValues": {":v": {"N": "2"}}}
```

The first line has no condition, so it overwrites the existing item and Product 5 moves to `Version 2`. The second line is written only if the item doesn't exist. Product 20005 exists, so it is skipped. The third line is a version guard. It is written only if its `Version` is higher than the table's, so old data can't replace newer data. Here 2 is higher than 1, so it is written.

One thing to watch with the version guard: `#v < :v` on its own also skips new keys, because a missing `Version` can't be compared. To write both new keys and newer versions, AWS uses `attribute_not_exists(#v) OR #v < :v` ([Considerations](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Considerations.html)).

## Starting a load

One command starts a load. It names the table, the input files and the output location:

```bash
aws dynamodb start-bulk-put-items \
    --table-name BulkPutStandardTable \
    --input-s3-configuration '{"S3Bucket":"bulk-put-input-111122223333-us-east-1","S3KeyPrefix":"batches/conflict-test/data/","InputFormat":"DYNAMODB_JSON","InputCompressionType":"GZIP"}' \
    --output-s3-configuration '{"S3Bucket":"bulk-put-output-111122223333-us-east-1","S3KeyPrefix":"bulk-put-output/","OutputType":["ERROR","CONDITION_CHECK_FAILURE"]}' \
    --performance-tier STANDARD
```

Use `ACCELERATED` for the Accelerated tier. If you pass a client token, a retried start returns the existing operation instead of starting a second one. DynamoDB keeps the token for 8 hours.

## Reports and statuses

DynamoDB writes a manifest to the output location, which says whether each input file was read in full. It also writes up to two reports: `ERROR`, for records with bad data such as a missing key, and `CONDITION_CHECK_FAILURE`, for records skipped because their condition was false. You only get the second one if you ask for it in `OutputType`.

An operation ends in one of four statuses. `COMPLETED` means no record failed, though some may still have been skipped by a condition. `COMPLETED_WITH_ERRORS` means some records failed, such as a record with no key. `CANCELLED` means it was stopped on request, and the items it already wrote stay in the table. `FAILED` means it couldn't finish, and the failure code says why.

## Sample project

The CDK project loads the same data in three ways, so the only difference is the method. It deploys:

- an input bucket for the product records, in gzip files
- an output bucket for the bulk put reports and manifest, plus a summary file for each run
- three on-demand tables: `BatchWriteTable`, `BulkPutStandardTable` and `BulkPutAcceleratedTable`
- three Step Functions workflows, one per table, started with the same input
- a CloudWatch dashboard, `BulkPutComparison`, with items loaded, table capacity used and throttles for all three tables

### BatchWrite

This workflow writes the items using `BatchWriteItem`, 20 files at a time.

![BatchWrite workflow](https://raw.githubusercontent.com/ssathasivan/amazon_dynamodb_bulk_put_items/main/BatchWrite.png)

### BulkPutStandard

This workflow writes the items using bulk put items on the Standard tier.

![BulkPutStandard workflow](https://raw.githubusercontent.com/ssathasivan/amazon_dynamodb_bulk_put_items/main/BulkPutStandard.png)

### BulkPutAccelerated

This workflow writes the items using bulk put items on the Accelerated tier.

![BulkPutAccelerated workflow](https://raw.githubusercontent.com/ssathasivan/amazon_dynamodb_bulk_put_items/main/BulkPutAccelerated.png)

### Why a Lambda starts the load

We wanted Step Functions to call `StartBulkPutItems` directly, but it can't yet. On November 11, 2026, `aws stepfunctions validate-state-machine-definition` in us-east-1 rejected `arn:aws:states:::aws-sdk:dynamodb:startBulkPutItems` with "The resource provided … is not recognized", so a Lambda makes the call. AWS adds SDK integrations over time, so check again before you copy this design.

The waiting happens in Step Functions, not in the Lambda. The workflow checks `DescribeBulkOperation` every 30 seconds, instead of one Lambda waiting for many minutes.

We also split the input into equal files with shuffled keys, because AWS says bulk put items works in parallel across files and needs evenly spread keys ([Best practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.BestPractices.html)). We start all three workflows manually, at the same time and with the same files, so the method is the only difference. The first test uses no conditions, because `BatchWriteItem` can't use them and we wanted a fair comparison.

### Permissions

Bulk put items runs with the caller's permissions, so the start Lambda's role has:

- `dynamodb:StartBulkOperation` and `dynamodb:BulkPutItems`, with `BulkPutItems` allowed only on the two bulk put tables
- `dynamodb:DescribeBulkOperation`, `dynamodb:CancelBulkOperation` and `dynamodb:ListBulkOperations`, without which you can start an operation but not monitor or cancel it
- `s3:GetObject` and `s3:ListBucket` on the input bucket
- `s3:PutObject` and `s3:AbortMultipartUpload` on the output bucket

The buckets use S3-managed encryption, so the role needs no `kms:` permissions. With a customer managed KMS key on the table or the objects, add `kms:Decrypt` and `kms:GenerateDataKey`. Without `kms:Decrypt`, the start fails with `AccessDeniedException` ([Considerations](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Considerations.html)).

### Deploying

You need AWS credentials in your shell, Python 3.10 or later, Node.js and the CDK CLI (`npm i -g aws-cdk@latest`). Use a Region that has the Accelerated tier. We ran everything in us-east-1.

```bash
git clone https://github.com/ssathasivan/amazon_dynamodb_bulk_put_items.git
cd amazon_dynamodb_bulk_put_items
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\Activate.ps1
pip install -r infra/requirements.txt -r app/requirements.txt

cd infra
cdk bootstrap                        # once per account and Region
cdk deploy
```

The stack bundles boto3 1.43.124 or later into the Lambda package, because the Lambda runtime's own boto3 may not have `start_bulk_put_items` yet.

## Test 1: 1 million items

The first test loads 1 million products of about 1 KB, in 20 gzip files of 50,000 records, so about 1 GB. The records are plain writes with no condition. We ran it twice: first into the empty tables, then again into the same tables, so every record replaces an existing item.

This creates the files and uploads them to `s3://<InputBucketName>/batches/catalog-1m/data/`:

```bash
cd ../app
python -m bulkput generate --count 1000000 --batch catalog-1m --shards 20
```

This starts all three workflows with the same input. Run it a second time for the second run:

```bash
python -m bulkput start --batch catalog-1m
```

This prints the load time, items per second, cost, capacity used and throttles of each workflow's latest run. CloudWatch metrics can lag, so run it again if the capacity looks low:

```bash
python -m bulkput compare
```

### Results

**Run 1: new tables**

| Method | Load time | Items/s | Table capacity used | Write throttles |
|---|---:|---:|---:|---:|
| Bulk put, Standard | 23 min 5 s | 722 | 0 | 0 |
| Bulk put, Accelerated | 22 min 55 s | 727 | 0 | 0 |
| BatchWriteItem | 4 min 14 s | 3,937 | 1,000,000 | 387,018 |

**Run 2: same tables**

| Method | Load time | Items/s | Table capacity used | Write throttles |
|---|---:|---:|---:|---:|
| Bulk put, Standard | 8 min 46 s | 1,901 | 0 | 0 |
| Bulk put, Accelerated | 12 min 17 s | 1,357 | 0 | 0 |
| BatchWriteItem | 1 min 12 s | 13,889 | 1,000,000 | 17,833 |

Capacity and throttles come from the table's CloudWatch metrics. Every load wrote all 1 million items.

`BatchWriteItem` was 5 times faster in the first run, and 7 to 10 times faster in the second. But it used 1 million capacity units and was throttled in both runs. Both bulk put tables show 0 capacity used and 0 throttles, because bulk writes don't use the table's capacity ([Considerations](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Considerations.html)).

Accelerated was no faster than Standard. The two were about the same in the first run, and Accelerated was slower in the second. AWS recommends Standard for loads under 1 GB ([AWS Database Blog](https://aws.amazon.com/blogs/database/introducing-bulk-put-items-for-amazon-dynamodb-load-data-from-amazon-s3-into-existing-tables-at-scale/)), so we tried 10 GB in the third test.

Every method was faster the second time. For bulk put items, AWS says the first operations on a table can outpace table scaling ([Performance tiers](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Tiers.html)). `BatchWriteItem` most likely got faster, and was throttled far less, because the on-demand table scaled up after the first run, as we saw in the third test.

## Test 2: conditions and the report

The second test checks conditions and the report. It runs only the two bulk put workflows, on the tables from the first test, which hold the 1 million products at `Version` 1. The input is 100,010 records in 4 gzip files:

| Records | Condition | Expected |
|---|---|---|
| 20,000 existing keys, `Version` 2 | none | written |
| 20,000 existing keys | `attribute_not_exists(sku)` | skipped |
| 20,000 new keys | `attribute_not_exists(sku)` | written |
| 20,000 existing keys, `Version` 2 | `#v < :v`, with `:v` = 2 | written |
| 20,000 existing keys, `Version` 1 | `#v < :v`, with `:v` = 1 | skipped |
| 10 records with no `sku` | none | in the error report |

So we expect 60,000 written, 40,000 condition failures and 10 errors, with every touched key at the expected `Version`.

The README has the commands that reset the tables and upload the 100,010 records. This starts only the two bulk put workflows:

```bash
python -m bulkput start --batch conflict-test --bulk-only
```

This compares the report counts with the plan, reads all 100,000 touched keys, and checks each `Version`. It ends with `Result: matches the plan` or `Result: DOES NOT match the plan`:

```bash
python -m bulkput conflict-check
```

### Results

Both loads ended with the status `COMPLETED_WITH_ERRORS`, and both matched the plan.

| | Standard | Accelerated |
|---|---:|---:|
| Load time | 3 min 25 s | 2 min 39 s |
| Records read | 100,010 | 100,010 |
| Written | 60,000 | 60,000 |
| Condition failures | 40,000 | 40,000 |
| Errors | 10 | 10 |
| Files fully processed | 4 of 4 | 4 of 4 |
| Keys with the expected `Version` | 100,000 of 100,000 | 100,000 of 100,000 |

`DescribeBulkOperation` gave us `ProcessedItemCount` of 100,010, which includes the skipped and failed records, and `FailedItemCount` of 10, which counts only the errors. It has no count for condition failures, so the 40,000 came from the report files.

Each report record points back to its source: the input file, the position (`itemIndex`) and the reason, for example "Missing the key sku in the item". Condition failures also carry the item you sent (`newImage`) and the item in the table (`oldImage`). With the position, you can fix the bad records and load only those again. A report file is only written when it has records, so the first test produced only the manifest.

## Test 3: 10 million items

The third test repeats the first at 10 times the size, to see whether Accelerated helps above 1 GB. It also adds app traffic, to see whether a load affects an application that uses the same table. It loads 10 million products, about 1 KB each, in 100 gzip files of 100,000 records, so about 10 GB in total. The records are plain writes again.

We ran the loads three times without app traffic: first into new, empty tables, then twice into the same tables. Then we redeployed for new tables and ran a fourth load with app traffic.

The app traffic comes from an `AppTraffic` Step Functions workflow, which runs a Lambda against each table in 10-minute chunks. It sends 200 `GetItem` requests per second to random products, and 50 `PutItem` requests per second to separate `app-` keys, so it never changes the loaded items. It makes one attempt per request, with no SDK retries, so every throttle is counted. Every minute it publishes the latency and the throttles to CloudWatch.

This creates the files and uploads them to `s3://<InputBucketName>/batches/catalog-10m/data/`:

```bash
python -m bulkput generate --count 10000000 --batch catalog-10m --shards 100
```

For the runs without app traffic, run this three times, and run `compare` after all three workflows finish each time:

```bash
python -m bulkput start --batch catalog-10m
python -m bulkput compare
```

For the run with app traffic, start the traffic on new tables and wait 10 minutes for a baseline. Then start the loads, and when they finish, compare the app's traffic in the baseline with its traffic during each load:

```bash
python -m bulkput traffic-start --minutes 180 --reads 200 --writes 50
python -m bulkput start --batch catalog-10m
python -m bulkput traffic-report
python -m bulkput traffic-stop
```

### Results without app traffic

Every load wrote all 10 million items.

| Method | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| Bulk put, Standard | 2 h 8 min 16 s | 30 min 7 s | 23 min 2 s |
| Bulk put, Accelerated | 1 h 43 min 40 s | 30 min 32 s | 22 min 12 s |
| BatchWriteItem | 28 min 23 s | 10 min 43 s | 5 min 31 s |

| Method | Items/s, run 1 / 2 / 3 | Table capacity used per run | Write throttles, run 1 / 2 / 3 |
|---|---:|---:|---:|
| Bulk put, Standard | 1,299 / 5,534 / 7,236 | 0 | 0 / 0 / 0 |
| Bulk put, Accelerated | 1,608 / 5,459 / 7,508 | 0 | 0 / 0 / 0 |
| BatchWriteItem | 5,872 / 15,552 / 30,211 | about 10,000,000 | 1,001,970 / 130,900 / 41,412 |

`BatchWriteItem` was still faster: about 4 times faster than bulk put items in the first and third runs, and about 3 times in the second.

Accelerated helped only on the first load into new tables, where it was about 1.2 times faster than Standard. In the second and third runs, the two tiers took about the same time.

Every run was faster than the one before. Standard went from over 2 hours to 23 minutes and was still falling. AWS says Accelerated times typically settle after three to five runs ([AWS Database Blog](https://aws.amazon.com/blogs/database/introducing-bulk-put-items-for-amazon-dynamodb-load-data-from-amazon-s3-into-existing-tables-at-scale/)), so we may not have seen the final speed. We also didn't reach AWS's estimate of less than 10 minutes for 10 million 1 KB items on Accelerated, on a well-scaled table ([Performance tiers](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Tiers.html)). Our fastest Accelerated run took 22 min 12 s.

After the first run, `describe-table` showed both bulk put tables still at 4,000 write units per second, the default for a new on-demand table, while `BatchWriteTable` had grown to 15,668. Bulk put items still got faster, so that number doesn't show its scaling.

### Results with app traffic

| Table | Window | App requests | App write throttles | App read throttles | Read p50 | Write p50 |
|---|---|---:|---:|---:|---:|---:|
| BatchWriteTable | baseline, 10 min | 149,856 | 0 | 0 | 2.9 ms | 4.1 ms |
| BatchWriteTable | during `BatchWriteItem`, 29 min | 435,000 | 1,787 | 0 | 3.2 ms | 4.1 ms |
| BulkPutStandardTable | baseline, 10 min | 149,998 | 0 | 0 | 3.3 ms | 5.7 ms |
| BulkPutStandardTable | during Standard, 126 min | 1,874,336 | 0 | 0 | 3.4 ms | 4.3 ms |
| BulkPutAcceleratedTable | baseline, 10 min | 149,999 | 0 | 0 | 3.4 ms | 5.4 ms |
| BulkPutAcceleratedTable | during Accelerated, 117 min | 1,754,996 | 0 | 0 | 3.2 ms | 4.1 ms |

During the `BatchWriteItem` load, about 2% of the app's writes were throttled: 1,787 of about 87,000. Its reads were not. During both bulk put loads, which ran for about 2 hours each, the app had no throttles at all. That is our own evidence for AWS's claim that bulk writes don't compete with your application.

Median latency didn't change in any window. We can't use the p99 latency: it was 120 to 230 ms even in the baseline, far above normal DynamoDB latency, so it most likely shows queueing in the Python Lambda that sends the traffic, not in DynamoDB.

Each load window also had 13 to 53 errors that weren't throttles, at most 0.003% of its requests, and the baselines had none. The code didn't record the error type, so we don't know the cause.

The loads took 2 h 7 min on Standard, 1 h 58 min on Accelerated and 30 min with `BatchWriteItem`, close to the first run without app traffic. `BatchWriteItem` itself was throttled 1,162,742 times. The bulk put tables show about 380,000 to 410,000 write units used this time, but those are the app's own writes, not the load.

Test 1 ran twice, test 2 once and test 3 four times, so these results should be treated as directional.

## Clean up

```bash
cd ../infra
cdk destroy
```

The buckets use `auto_delete_objects` and the tables use `RemovalPolicy.DESTROY`, so one command removes everything. Don't copy those settings into a production stack.

## Approximate cost

The following cost is calculated for one run of the third test in us-east-1. The bulk put cost uses the bytes that `DescribeBulkOperation` reported in `BulkOperationUsage`.

Assumptions:

- Standard table class. Standard-IA tables have different rates.
- 10 million items of about 1 KB, no secondary indexes
- metered bytes converted to GiB (1 GiB = 1,073,741,824 bytes), as the sample's code does

| Component | Calculation | Cost |
|---|---|---|
| Bulk put, Standard | about 9.5 GiB metered × $0.10 | $0.95 |
| Bulk put, Accelerated | about 9.5 GiB metered × $0.20 | $1.91 |
| BatchWriteItem, on-demand | 10,000,000 write units ÷ 1,000,000 × $0.625 | $6.25 |

AWS doesn't say whether its GB is 1,000,000,000 or 1,073,741,824 bytes. With 1,000,000,000, the same bytes come to $1.02 and $2.05.

Bulk put items is billed per GB written, not per request unit, so the cost scales with the data. On the same GiB basis, 100 million 1 KB items would cost about $9.54 on Standard and $19.07 on Accelerated, against $62.50 with on-demand writes. AWS's own example prices them at $10.00 and $20.00, as 100 GB.

The second test cost about $0.01 on Standard and $0.02 on Accelerated. It paid for all 100,000 valid records, not only the 60,000 written, because condition failures are billed too. The 10 records that failed validation were not billed.

The run with app traffic cost the same as the other 10 million runs for the loads, plus an estimated $2 for the app traffic: about 6.5 million eventually consistent reads, 1.6 million writes, and three Lambda functions running for 3 hours.

Every item is billed at a minimum of 1 KB, for the table and for each index. Above that you pay for the actual size, so a 1.1 KB item is billed as 1.1 KB, where a write unit rounds it up to 2 KB.

To estimate your own load:

```bash
python -m bulkput cost --items 100000000 --item-bytes 1024
```

Rates for other Regions and for Standard-IA tables are on the [DynamoDB pricing page](https://aws.amazon.com/dynamodb/pricing/).

## Gotchas

- Don't share a client token between tables. We first used the Step Functions execution name as the token for both bulk put workflows, and the second start failed with `IdempotentParameterMismatchException`. The fix was `<workflow name>-<execution name>`.
- Table metrics show nothing during a load, so the check Lambda publishes its own `BulkPutDemo/ItemsProcessed` metric for the dashboard.
- The reports are not next to the manifest. The manifest is in `<operation-id>/`, but the report files are in `<operation-id>-0/`, `<operation-id>-1/` and so on, one per internal job. A listing that ends in `<operation-id>/` finds only the manifest, so drop the trailing slash.
- Ask for `CONDITION_CHECK_FAILURE` whenever your records have conditions. Without it, you have no record or count of skipped items at all.
- After an internal failover, DynamoDB can write a record a second time. A record with no condition can then overwrite a newer write from your application, so put a version guard on every record.

For the size limits, quotas, table states and unsupported features such as global tables, see [Considerations](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Considerations.html).

## Pros and cons

| Pros | Cons |
|---|---|
| 0 capacity used and 0 throttles in every run of tests 1 and 3, and 0 app throttles during the bulk put loads, against 1,787 during `BatchWriteItem` | 3 to 10 times slower than `BatchWriteItem` in tests 1 and 3 |
| Conditions per record, which skipped exactly the 40,000 records they should have | Accelerated was faster only on the first 10 GB load and in the small second test, from one run each, at twice the price |
| A report with each failed record's file and position | The first 10 million-item load took about 2 hours on Standard, against 23 to 30 minutes later |
| About 6.6 times cheaper than on-demand writes on Standard | No condition-failure count, and reports split across folders |
| One API call and a status check, instead of a loader | No global tables |

## Conclusion

Bulk put items kept the bulk put loads at zero throttles and checked every record's condition, which a `BatchWriteItem` loader can't do. With app traffic on the table, the app saw no throttles during the bulk put loads, while about 2% of its writes were throttled during the `BatchWriteItem` load. It was also slower than `BatchWriteItem` in every test where we compared them, and Accelerated rarely earned its double price.

Worth trying, but start with Standard and measure it on your own data before you commit to it.

## References

- [Amazon DynamoDB now supports bulk put items](https://aws.amazon.com/about-aws/whats-new/2026/11/amazon-dynamodb-now-supports-bulk-put-items/)
- [Introducing bulk put items for Amazon DynamoDB (AWS Database Blog)](https://aws.amazon.com/blogs/database/introducing-bulk-put-items-for-amazon-dynamodb-load-data-from-amazon-s3-into-existing-tables-at-scale/)
- [Working with bulk put items](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.HowItWorks.html)
- [Performance tiers](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Tiers.html)
- [Amazon S3 input formats for bulk put items](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Format.html)
- [Running a bulk put items operation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Requesting.html)
- [Bulk put items output and reconciliation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Output.html)
- [Bulk put items best practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.BestPractices.html)
- [Bulk put items considerations, quotas, and permissions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BulkPutItems.Considerations.html)
- [Amazon DynamoDB can now import Amazon S3 data into a new table](https://aws.amazon.com/blogs/database/amazon-dynamodb-can-now-import-amazon-s3-data-into-a-new-table/)
- [Amazon DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/)
