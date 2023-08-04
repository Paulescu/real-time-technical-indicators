.PHONY: init debug run backfill

# install Poetry and Python dependencies
init:
	curl -sSL https://install.python-poetry.org | python3 -
	poetry install

# run the feature-pipeline locally and print out the results on the console
debug:
	poetry run python -m bytewax.run "src.dataflow:get_dataflow(execution_mode='DEBUG')"
	
# run the feature-pipeline and send the feature to the feature store
run:
	poetry run python -m bytewax.run "src.dataflow:get_dataflow()"

# backfills the feature group using historical data
backfill:
	poetry run python src/backfill.py --from_day $(from_day) --product_id XBT/USD