#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.bulk_put_stack import BulkPutStack

app = cdk.App()
BulkPutStack(
    app,
    "BulkPutDemoStack",
    # Resolve account/Region from the CLI profile so the stack deploys wherever you are logged in.
    env=cdk.Environment(
        account=app.node.try_get_context("account") or None,
        region=app.node.try_get_context("region") or None,
    ),
)
app.synth()
