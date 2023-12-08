# paradime-dagster-dbt

This is Paradime's integration of dbt™ with Dagster and is a fork of Dagster's repo https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-dbt

The docs for `dagster-dbt` repo can be found
[here](https://docs.dagster.io/_apidocs/libraries/dagster-dbt).

# Setting Up Paradime in Dagster Locally

1. Setup a virtual environment first for `paradime-dagster-dbt`
   ```shell
   # this creates the virtual environment
   $ python -m venv paradime-dagster-env

   # activate the virtual environment
   $ . paradime-dagster-env/bin/activate
   ```

2. Install the `paradime-dagster-dbt` python package and the `dagster-webserver` using `pip`:
   ```shell
   # install paradime-dagster-dbt
   $ pip install -e git+https://github.com/paradime-io/paradime-dagster-dbt.git#egg=dagster_dbt

   # install Dagster webserver
   $ pip install dagster-webserver==1.5.9
   ```
   
3. Set up your dagster project (if not already):
   ```shell
   $ dagster project scaffold --name paradime-dagster-demo
   ```

4. Set up the Paradime definitions in your dagster project in the `__init__.py` file (i.e `paradime-dagster-demo/paradime_dagster_demo/__init__.py`) as follows:
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
       schedule_name="daily_run",  # Your schedule name in paradime (Must not include spaces)
   )
   
   defs = Definitions(
       assets=[paradime_assets]
   )

   ```
   
5. Export your [API env variables](https://docs.paradime.io/app-help/app-settings/generate-api-keys). e.g:
   ```shell
   $ export PARADIME_API_KEY=...
   $ export PARADIME_API_SECRET=...
   $ export PARADIME_API_HOST=...
   ```

6. Now you can run dagster inside your dagster directory:
   ```shell
   $ cd paradime-dagster
   $ dagster dev -h 127.0.0.1
   ```

7. Finally, open the Dagster UI: [http://127.0.0.1:3000](http://127.0.0.1:3000)

# Cloud Deployment

Ensure that you update the `setup.py` by adding the `paradime-dagster-dbt` python package to the `install_requires` list. For example:
```python
setup(
    name="paradime_dagster_demo",
    packages=find_packages(exclude=["paradime_dagster_demo_tests"]),
    install_requires=[
        "dagster-dbt @ git+https://github.com/paradime-io/paradime-dagster-dbt.git@1.5.9#egg=dagster_dbt",
        "dagster==1.5.9",
        "dagster-cloud==1.5.9",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
```

You also need to [setup your environment variables](https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets) in dagster.


## Serverless
You will need to have a `dagster_cloud.yaml` file. Something like this inside the `paradime-dagster-demo` directory:
```yaml
locations:
  - location_name: paradime-dagster-demo
    code_source:
      package_name: paradime_dagster_demo
```

After setting up the [dagster-cloud cli](https://docs.dagster.io/dagster-cloud/managing-deployments/dagster-cloud-cli). You can then run a serveless deploy by running this inside the `paradime-dagster-demo` directory:
```shell
$ dagster-cloud serverless deploy \
  --location-name=paradime-dagster-demo \
  --location-file dagster_cloud.yaml \
  --base-image python:3.8 # needed to install git
```