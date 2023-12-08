# paradime-dagster-dbt

This is Paradime's integration of dbt™ with Dagster and is a fork of Dagster's repo https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-dbt

The docs for `dagster-dbt` repo can be found
[here](https://docs.dagster.io/_apidocs/libraries/dagster-dbt).

# Setting Up Paradime in Dagster Locally

1. Setup a virtual environment first for `paradime-dagster-dbt`
   ```
   # this creates the virtual environment
   $ python -m venv paradime-dagster-env

   # activate the virtual environment
   $ . paradime-dagster-env/bin/activate
   ```

2. Install the `paradime-dagster-dbt` python package and the `dagster-webserver` using `pip`:
   ```
   # install paradime-dagster-dbt
   $ pip install -e git+https://github.com/paradime-io/paradime-dagster-dbt.git#egg=dagster_dbt

   # install Dagster webserver
   $ pip install dagster-webserver==1.5.9
   ```
   
3. Set up your dagster project (if not already):
   ```
   $ dagster project scaffold --name paradime-dagster-demo
   ```

4. Set up the Paradime definitions in your dagster project in the `__init__.py` file (i.e `paradime-dagster-demo/paradime_dagster-demo/__init__.py`) as follows:
   ```python
   import os
   from dagster import Definitions
   from dagster_dbt import ParadimeClientResource
   from dagster_dbt import load_assets_from_paradime_schedule
   
   # These env variables can be found by adding an API Key (Ref: https://docs.paradime.io/app-help/app-settings/generate-api-keys)
   paradime = ParadimeClientResource(
       api_key=os.getenv("PARADIME_API_KEY"),
       api_secret=os.getenv("PARADIME_API_SECRET"),
       paradime_api_host=os.getenv("PARADIME_API_HOST"),
   )
   
   paradime_assets = load_assets_from_paradime_schedule(
       paradime=paradime,
       schedule_name="daily_run",  # Your schedule name in paradime
   )
   
   defs = Definitions(
       assets=[paradime_assets]
   )

   ```
   
5. Export your [API env variables](https://docs.paradime.io/app-help/app-settings/generate-api-keys). e.g:
   ```
   $ export PARADIME_API_KEY=...
   $ export PARADIME_API_SECRET=...
   $ export PARADIME_API_HOST=...
   ```

6. Now you can run dagster inside your dagster directory: `$ dagster dev -h 127.0.0.1`

7. Finally, open the Dagster UI: [http://127.0.0.1:3000](http://127.0.0.1:3000)
