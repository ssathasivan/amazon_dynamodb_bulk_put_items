"""Infrastructure for the bulk put items comparison.

One set of input files, three workflows, three empty tables:

  BulkPutStandard     -> BulkPutStandardTable      bulk put items, Standard tier
  BulkPutAccelerated  -> BulkPutAcceleratedTable   bulk put items, Accelerated tier
  BatchWrite          -> BatchWriteTable           BatchWriteItem from Lambda

The BulkPutComparison CloudWatch dashboard charts all three side by side.

You start the three workflows manually, with the same input:

  {"bucket": "<input bucket>", "prefix": "batches/<id>/data/"}

`python -m bulkput start` does this for all three at once.

Bulk put items runs with the CALLER's credentials. The caller is StartFunction's role.
"""
from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cw,
    aws_dynamodb as ddb,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct

from .lambda_asset import build_lambda_asset

BULK_STANDARD_TABLE = "BulkPutStandardTable"
BULK_ACCELERATED_TABLE = "BulkPutAcceleratedTable"
BATCH_TABLE = "BatchWriteTable"


class BulkPutStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---------------------------------------------------------------- buckets
        def make_bucket(cid: str, name: str, expire_days: int) -> s3.Bucket:
            return s3.Bucket(
                self,
                cid,
                bucket_name=f"{name}-{self.account}-{self.region}",
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                encryption=s3.BucketEncryption.S3_MANAGED,
                enforce_ssl=True,
                removal_policy=RemovalPolicy.DESTROY,  # demo only
                auto_delete_objects=True,  # demo only: lets `cdk destroy` empty the bucket
                lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(expire_days))],
            )

        input_bucket = make_bucket("InputBucket", "bulk-put-input", expire_days=7)
        output_bucket = make_bucket("OutputBucket", "bulk-put-output", expire_days=30)

        # ----------------------------------------------------------------- tables
        def make_table(name: str) -> ddb.Table:
            return ddb.Table(
                self,
                name,
                table_name=name,
                partition_key=ddb.Attribute(name="sku", type=ddb.AttributeType.STRING),
                billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
                removal_policy=RemovalPolicy.DESTROY,  # demo only
            )

        bulk_standard_table = make_table(BULK_STANDARD_TABLE)
        bulk_accelerated_table = make_table(BULK_ACCELERATED_TABLE)
        batch_table = make_table(BATCH_TABLE)

        # -------------------------------------------------------------- functions
        code = lambda_.Code.from_asset(build_lambda_asset())

        def make_fn(cid: str, handler: str, description: str, timeout: Duration, memory: int = 1024) -> lambda_.Function:
            return lambda_.Function(
                self,
                cid,
                runtime=lambda_.Runtime.PYTHON_3_13,
                handler=f"bulkput.handler.{handler}",
                code=code,
                memory_size=memory,
                timeout=timeout,
                environment={"OUTPUT_BUCKET": output_bucket.bucket_name},
                description=description,
                log_group=logs.LogGroup(
                    self, f"{cid}Logs", retention=logs.RetentionDays.ONE_WEEK, removal_policy=RemovalPolicy.DESTROY
                ),
            )

        start_fn = make_fn("StartFunction", "start", "Bulk put items: starts the operation", Duration.minutes(1))
        check_fn = make_fn("CheckFunction", "check", "Bulk put items: checks status, then reconciles", Duration.minutes(10))
        batch_fn = make_fn("BatchWriteFunction", "batch", "BatchWriteItem comparison load", Duration.minutes(15), 2048)
        traffic_fn = make_fn("AppTrafficFunction", "traffic", "Test setup 4: steady app reads and writes", Duration.minutes(15), 1024)

        # ------------------------------------------------------------ permissions
        # StartFunction is the bulk put items caller. All five DynamoDB actions are required:
        # a policy with only the start actions can start an operation but not describe or cancel it.
        bulk_ops = self.format_arn(service="dynamodb", resource="bulk-operation", resource_name="*")
        iam.ManagedPolicy(
            self,
            "BulkCallerPolicy",
            roles=[start_fn.role],
            statements=[
                iam.PolicyStatement(actions=["dynamodb:StartBulkOperation"], resources=[bulk_ops]),
                iam.PolicyStatement(
                    actions=["dynamodb:BulkPutItems"],
                    resources=[bulk_standard_table.table_arn, bulk_accelerated_table.table_arn],
                ),
                iam.PolicyStatement(
                    actions=[
                        "dynamodb:DescribeBulkOperation",
                        "dynamodb:CancelBulkOperation",
                        "dynamodb:ListBulkOperations",
                    ],
                    resources=[bulk_ops],
                ),
                iam.PolicyStatement(
                    actions=["s3:GetObject", "s3:ListBucket"],
                    resources=[input_bucket.bucket_arn, input_bucket.arn_for_objects("*")],
                ),
                iam.PolicyStatement(
                    actions=["s3:PutObject", "s3:AbortMultipartUpload"],
                    resources=[output_bucket.arn_for_objects("*")],
                ),
            ],
        )
        check_fn.add_to_role_policy(iam.PolicyStatement(actions=["dynamodb:DescribeBulkOperation"], resources=[bulk_ops]))
        check_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],  # PutMetricData has no resource ARN; the condition limits the namespace
                conditions={"StringEquals": {"cloudwatch:namespace": "BulkPutDemo"}},
            )
        )
        output_bucket.grant_read_write(check_fn)

        input_bucket.grant_read(batch_fn)
        output_bucket.grant_write(batch_fn)
        batch_table.grant_write_data(batch_fn)

        traffic_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:GetItem", "dynamodb:PutItem"],
                resources=[t.table_arn for t in (bulk_standard_table, bulk_accelerated_table, batch_table)],
            )
        )
        traffic_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={"StringEquals": {"cloudwatch:namespace": "BulkPutDemo"}},
            )
        )

        # ------------------------------------------------- bulk put items workflows
        def bulk_workflow(name: str, table: str, tier: str) -> sfn.StateMachine:
            start = tasks.LambdaInvoke(
                self,
                f"{name}Start",
                lambda_function=start_fn,
                payload=sfn.TaskInput.from_object(
                    {
                        "bucket": sfn.JsonPath.string_at("$.bucket"),
                        "prefix": sfn.JsonPath.string_at("$.prefix"),
                        # The client token is unique per account, not per table, so add the workflow name.
                        "token": sfn.JsonPath.format(
                            "{}-{}",
                            sfn.JsonPath.string_at("$$.StateMachine.Name"),
                            sfn.JsonPath.string_at("$$.Execution.Name"),
                        ),
                        "table": table,
                        "tier": tier,
                    }
                ),
                payload_response_only=True,
                result_path="$.op",
            )
            # One operation per table: if another one is running, wait and try again.
            start.add_retry(
                errors=["ResourceInUseException", "LimitExceededException"],
                interval=Duration.seconds(60),
                backoff_rate=1.5,
                max_attempts=10,
            )
            wait = sfn.Wait(self, f"{name}Wait30Seconds", time=sfn.WaitTime.duration(Duration.seconds(30)))
            check = tasks.LambdaInvoke(
                self,
                f"{name}CheckStatus",
                lambda_function=check_fn,
                payload=sfn.TaskInput.from_object({"arn": sfn.JsonPath.string_at("$.op.arn"), "table": table}),
                payload_response_only=True,
                result_path="$.check",
            )
            outcome = (
                sfn.Choice(self, f"{name}Succeeded?")
                .when(sfn.Condition.boolean_equals("$.check.result.ok", True), sfn.Succeed(self, f"{name}Done"))
                .otherwise(sfn.Fail(self, f"{name}NeedsAttention", error="BulkLoadNeedsAttention", cause="See $.check.result"))
            )
            finished = (
                sfn.Choice(self, f"{name}Finished?")
                .when(sfn.Condition.boolean_equals("$.check.done", False), wait)
                .otherwise(outcome)
            )
            return sfn.StateMachine(
                self,
                name,
                state_machine_name=name,
                definition_body=sfn.DefinitionBody.from_chainable(start.next(wait).next(check).next(finished)),
                timeout=Duration.hours(12),
            )

        standard_sm = bulk_workflow("BulkPutStandard", BULK_STANDARD_TABLE, "STANDARD")
        accelerated_sm = bulk_workflow("BulkPutAccelerated", BULK_ACCELERATED_TABLE, "ACCELERATED")

        # ------------------------------------------------- BatchWriteItem workflow
        def batch_call(cid: str, payload: dict, result_path: str | None = None) -> tasks.LambdaInvoke:
            task = tasks.LambdaInvoke(
                self,
                cid,
                lambda_function=batch_fn,
                payload=sfn.TaskInput.from_object(payload),
                payload_response_only=True,
                result_path=result_path,
            )
            task.add_retry(
                errors=["Lambda.TooManyRequestsException", "Lambda.ServiceException", "States.Timeout", "States.TaskFailed"],
                interval=Duration.seconds(10),
                backoff_rate=2,
                max_attempts=3,
            )
            return task

        list_files = batch_call(
            "BatchWriteListFiles",
            {"action": "list", "bucket": sfn.JsonPath.string_at("$.bucket"), "prefix": sfn.JsonPath.string_at("$.prefix"), "table": BATCH_TABLE},
            "$.list",
        )
        write_each = sfn.Map(
            self,
            "BatchWriteEachFile",
            items_path=sfn.JsonPath.string_at("$.list.files"),
            max_concurrency=20,
            item_selector={
                "action": "write",
                "bucket": sfn.JsonPath.string_at("$$.Map.Item.Value.bucket"),
                "key": sfn.JsonPath.string_at("$$.Map.Item.Value.key"),
                "table": sfn.JsonPath.string_at("$$.Map.Item.Value.table"),
            },
            result_path="$.results",
        )
        write_each.item_processor(
            batch_call(
                "BatchWriteFile",
                {
                    "action": sfn.JsonPath.string_at("$.action"),
                    "bucket": sfn.JsonPath.string_at("$.bucket"),
                    "key": sfn.JsonPath.string_at("$.key"),
                    "table": sfn.JsonPath.string_at("$.table"),
                },
            )
        )
        summarize = batch_call(
            "BatchWriteSummarize",
            {
                "action": "summarize",
                "results": sfn.JsonPath.object_at("$.results"),
                "started": sfn.JsonPath.string_at("$$.Execution.StartTime"),
                "execution": sfn.JsonPath.string_at("$$.Execution.Name"),
                "table": BATCH_TABLE,
            },
            "$.summary",
        )
        batch_sm = sfn.StateMachine(
            self,
            "BatchWrite",
            state_machine_name="BatchWrite",
            definition_body=sfn.DefinitionBody.from_chainable(list_files.next(write_each).next(summarize)),
            timeout=Duration.hours(12),
        )

        # ------------------------------------------- test setup 4: app traffic workflow
        # Runs the traffic Lambda in 10-minute chunks until the `until` time in the input.
        traffic_chunk = tasks.LambdaInvoke(
            self,
            "AppTrafficRunChunk",
            lambda_function=traffic_fn,
            payload=sfn.TaskInput.from_object(
                {
                    "table": sfn.JsonPath.string_at("$.table"),
                    "reads": sfn.JsonPath.number_at("$.reads"),
                    "writes": sfn.JsonPath.number_at("$.writes"),
                    "until": sfn.JsonPath.number_at("$.until"),
                }
            ),
            payload_response_only=True,
            result_path="$.last",
        )
        traffic_chunk.add_retry(
            errors=["Lambda.TooManyRequestsException", "Lambda.ServiceException", "Lambda.SdkClientException"],
            interval=Duration.seconds(5),
            backoff_rate=2,
            max_attempts=3,
        )
        traffic_loop = (
            sfn.Choice(self, "AppTrafficContinue?")
            .when(sfn.Condition.boolean_equals("$.last.continue", True), traffic_chunk)
            .otherwise(sfn.Succeed(self, "AppTrafficDone"))
        )
        traffic_sm = sfn.StateMachine(
            self,
            "AppTraffic",
            state_machine_name="AppTraffic",
            definition_body=sfn.DefinitionBody.from_chainable(traffic_chunk.next(traffic_loop)),
            timeout=Duration.hours(12),
        )

        # -------------------------------------------------------------- dashboard
        minute = Duration.minutes(1)

        def table_metric(table: str, name: str, label: str) -> cw.Metric:
            return cw.Metric(
                namespace="AWS/DynamoDB",
                metric_name=name,
                dimensions_map={"TableName": table},
                statistic="Sum",
                period=minute,
                label=label,
            )

        def bulk_progress(table: str, label: str) -> cw.Metric:
            return cw.Metric(
                namespace="BulkPutDemo",
                metric_name="ItemsProcessed",
                dimensions_map={"Table": table},
                statistic="Maximum",
                period=minute,
                label=label,
            )

        batch_wcu = table_metric(BATCH_TABLE, "ConsumedWriteCapacityUnits", "BatchWriteItem")
        items_loaded = cw.GraphWidget(
            title="Items loaded (running total)",
            left=[
                bulk_progress(BULK_STANDARD_TABLE, "Bulk put, Standard"),
                bulk_progress(BULK_ACCELERATED_TABLE, "Bulk put, Accelerated"),
                # Each item is under 1 KB, so 1 write capacity unit = 1 item written.
                cw.MathExpression(expression="RUNNING_SUM(wcu)", using_metrics={"wcu": batch_wcu}, label="BatchWriteItem", period=minute),
            ],
            width=24,
            height=8,
        )
        capacity = cw.GraphWidget(
            title="Table write capacity used per minute (bulk put should stay at 0)",
            left=[
                table_metric(BATCH_TABLE, "ConsumedWriteCapacityUnits", "BatchWriteTable"),
                table_metric(BULK_STANDARD_TABLE, "ConsumedWriteCapacityUnits", "BulkPutStandardTable"),
                table_metric(BULK_ACCELERATED_TABLE, "ConsumedWriteCapacityUnits", "BulkPutAcceleratedTable"),
            ],
            width=12,
            height=8,
        )
        throttles = cw.GraphWidget(
            title="Write throttles per minute",
            left=[
                table_metric(BATCH_TABLE, "WriteThrottleEvents", "BatchWriteTable"),
                table_metric(BULK_STANDARD_TABLE, "WriteThrottleEvents", "BulkPutStandardTable"),
                table_metric(BULK_ACCELERATED_TABLE, "WriteThrottleEvents", "BulkPutAcceleratedTable"),
            ],
            width=12,
            height=8,
        )
        def app_metric(table: str, name: str, statistic: str, label: str) -> cw.Metric:
            return cw.Metric(
                namespace="BulkPutDemo",
                metric_name=name,
                dimensions_map={"Table": table},
                statistic=statistic,
                period=minute,
                label=label,
            )

        all_tables = (BATCH_TABLE, BULK_STANDARD_TABLE, BULK_ACCELERATED_TABLE)
        app_latency = cw.GraphWidget(
            title="Test setup 4: app write latency p99 (ms)",
            left=[app_metric(t, "AppWriteLatency", "p99", t) for t in all_tables],
            width=12,
            height=8,
        )
        app_throttles = cw.GraphWidget(
            title="Test setup 4: app requests throttled per minute",
            left=[app_metric(t, "AppWriteThrottles", "Sum", f"{t} writes") for t in all_tables]
            + [app_metric(t, "AppReadThrottles", "Sum", f"{t} reads") for t in all_tables],
            width=12,
            height=8,
        )
        dashboard = cw.Dashboard(
            self,
            "BulkPutComparison",
            dashboard_name="BulkPutComparison",
            default_interval=Duration.hours(1),
            widgets=[[items_loaded], [capacity, throttles], [app_latency, app_throttles]],
        )

        # ---------------------------------------------------------------- outputs
        CfnOutput(
            self,
            "DashboardUrl",
            value=f"https://{self.region}.console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards/dashboard/{dashboard.dashboard_name}",
        )
        CfnOutput(self, "InputBucketName", value=input_bucket.bucket_name)
        CfnOutput(self, "OutputBucketName", value=output_bucket.bucket_name)
        CfnOutput(self, "BulkPutStandardArn", value=standard_sm.state_machine_arn)
        CfnOutput(self, "BulkPutAcceleratedArn", value=accelerated_sm.state_machine_arn)
        CfnOutput(self, "BatchWriteArn", value=batch_sm.state_machine_arn)
        CfnOutput(self, "AppTrafficArn", value=traffic_sm.state_machine_arn)
