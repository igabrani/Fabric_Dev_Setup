# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# Steps
# 1. Read data from Bronze Lakehouse
# 2. Fill out a test expectation suite
# 3. Create a test checkpoint (look into actions)
# 4. Do first validation. Organize those results and put them into DQ lakehouse
# 5. Save the context, set up another notebook that just reads the context
# 6. Build out expectations
# 7. Add logging

# MARKDOWN ********************

# # Validate Data (schema)

# MARKDOWN ********************

# ### First Time Setup

# CELL ********************

pip install great_expectations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations as gx

# Create context
context = gx.get_context()
# Create data source
data_source = context.data_sources.add_pandas(name = "bronze_raw_source")
# Create data asset
data_asset = data_source.add_dataframe_asset(name = "bronze_raw_asset")
# Create batch definition
batch_definition = data_asset.add_batch_definition_whole_dataframe(name = "bronze_raw_batch_definition")
# Get df_raw as a batch
batch_parameters = {"dataframe": df_raw}
batch = batch_definition.get_batch(batch_parameters = batch_parameters)

# Create an Expectation Suite
suite = gx.ExpectationSuite(name = "bronze_raw_expectation_suite")
# Add the Expectation Suite to the Data Context
suite = context.suites.add(suite)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test single expectations before putting them into 
# Create an Expectation to test
expectation = gx.expectations.ExpectColumnValueLengthsToEqual(
    column = "Member_ID",
    value = 11
)

# Test the Expectation
validation_results = batch.validate(expectation)
print(validation_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add the previously created Expectation to the Expectation Suite
suite.add_expectation(expectation)

# Update the configuration of an Expectation, then push the changes to the Expectation Suite
#expectation.column = "pickup_location_id"
#expectation.save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a validation definition
validation_definition = gx.ValidationDefinition(
    data = batch_definition, suite = expectation_suite, name = "bronze_raw_validation_definition"
)

# Run the validation definition
# Validate the dataframe by passing it to the Validation Definition as Batch Parameters.
validation_results = validation_definition.run(batch_parameters = batch_parameters)
print(validation_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
# Create a list of Actions for the Checkpoint to perform
action_list = [
    # This Action sends a Slack Notification if an Expectation fails.
    gx.checkpoint.SlackNotificationAction(
        name="send_slack_notification_on_failed_expectations",
        slack_token="${validation_notification_slack_webhook}",
        slack_channel="${validation_notification_slack_channel}",
        notify_on="failure",
        show_failed_expectations=True,
    ),
    # This Action updates the Data Docs static website with the Validation
    #   Results after the Checkpoint is run.
    gx.checkpoint.UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]

# Create the Checkpoint
checkpoint_name = "my_checkpoint"
checkpoint = gx.Checkpoint(
    name=checkpoint_name,
    validation_definitions=validation_definitions,
    actions=action_list,
    result_format={"result_format": "COMPLETE"},
)

# Save the Checkpoint to the Data Context
context.checkpoints.add(checkpoint)

# Run the checkpoint
validation_results = checkpoint.run(
    batch_parameters=batch_parameters, expectation_parameters=expectation_parameters
)
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
# Save context to lakehouse
context = context.convert_to_file_content()
!cp -r great_expectations/ /lakehouse/Data_Quality_Lakehouse
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Production Run

# CELL ********************

#Initialize context

#context_path = "/lakehouse/Data_Quality_Lakehouse/Files"

#context = gx.data_context.FileDataContext.create(project_root_dir = path_to_local_context)

# Retrieve a Validation Definition that uses the dataframe Batch Definition
#validation_definition_name = "bronze_raw_validation_definition"
#validation_definition = context.validation_definitions.get(validation_definition_name)

#batch_parameters = {"dataframe": df_raw}

# Validate the dataframe by passing it to the Validation Definition as Batch Parameters.
#validation_results = validation_definition.run(batch_parameters = batch_parameters)
#print(validation_results)

# OR

#validation_results = checkpoint.run(
#    batch_parameters=batch_parameters, expectation_parameters=expectation_parameters
#)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Handle Results

# CELL ********************

'''
# Use function to parse checkpoint results and load them to DQ lakehouse
# If Success, move on. If fail, handle failure
#def parse_and_load_checkpoint_results(results):
    validation_results = results['run_results'[next(iter(results['run_results']))]['validation_result']
    success = validation_results['success']

    restructured = {table of important key value pairs}

    write restructured to DQ lakehouse

    return success

success = parse_and_load

if success:
    move on 
else:
    raise exception
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
