# Kedro Glass

![](images/logo.png)


Glass is an extension of the popular data pipelining tool 
[Kedro](https://github.com/quantumblacklabs/kedro) aimed at providing users 
with a more flexible approach to building analytics applications. 

Instead of using Kedro's "configuration-as-code" system of explicit pipelines and 
nodes within python files, glass shifts much of this to a straight-forward 
configuration system. Developers can use a 
system of hooks to extend the behaviour of Kedro's default KedroContextin just a few lines. 

This allows for highly flexible production behaviour for applications which might 
serve many users with many use-cases. If you're developing an application with 
this requirement, `kedro-glass` might be for you!

## TLDR Illustration
Instead of:
```python
from kedro.pipeline import Pipeline, node

from my_project import my_function


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(my_function, "df_in", "df_out", name="my_function"),
        ],
        tags=["my_pipeline"]
    )
```

You declare nodes like so:
```yaml
my_function:
  func: my_project.my_function
  inputs: df_in
  outputs: df_out
```

And pipelines like so:
```yaml
my_pipeline:
  nodes:
    - my_function
```

Additionally, if you ever wanted to run pipelines based on different environments, you can now do this 
by including the relevant pipelines under the respective environment folder:
```text
my_project
    conf
        env1
            pipelines.yml
        env2
            pipelines.yml
    src
```
Running `kedro run --pipeline=env1` will now run the pipelines specified in `env1/pipelines.yml` 
and `kedro run --pipeline=env2` will run only the pipelines specified in `env2/pipelines.yml`.

## FAQ

## How does `kedro-glass` work?

`kedro-glass` works by scanning the configurations directories for pipeline and 
node configurations. It then builds kedro pipeline objects from the 
pipeline and node configurations that are compatible with all other kedro functionality. 

It is recommended that `kedro-glass` is integrated using the hooks system, 
available with kedro 0.17+.

## How do I integrate `kedro-glass` into my kedro project?

In order to integrate `kedro-glass` into your kedro project, you need to 
incorporate the relevant functions into the `hooks.py` file within your kedro project. 


## Is all of the kedro functionality supported?

Yes. 

`kedro-glass` supports all kedro pipeline options, including the use of kedro reusable 
pipelines. To use these features, you can optionally include them as dictionary 
arguments in your pipeline definitions. 
 
 For more information on how this functionality works, (including namespaces, 
 inputs, outputs, parameters and more) see 
 [the kedro documentation](https://kedro.readthedocs.io/en/stable/04_user_guide/06_pipelines.html#connecting-existing-pipelines)

The example below shows how to use tags, inputs, outputs and namespacing features. 

```yaml
combined_pipeline:
  pipelines:
    - data_engineering_pipeline
    - data_science_pipeline
  tags: 
    - data_science
  inputs:
    data_science_input_1: data_engineering_ouput_1
  outputs:
    data_science_output: pipeline_ouput
  namespace: end_to_end
  

data_engineering_pipeline:
    nodes:
      - de_node_1
      - de_node_2

data_science_pipeline:
    nodes:
      - ds_node_1
      - ds_node_2
```

Furthermore, by using a mixin system, `kedro-glass` is designed to maximise compatibility 
with the ecosystem of other kedro plugins. 

## How can I get started

The `get_glass_pipelines` function allows for the creation of pipelines and nodes 
from configuration. To use this mixin, follow the steps below. 

#### Step 1. Configure your kedro project's hooks to use the `kedro-glass` functionality. 

To use glass, you must correctly configure your projects hooks to invoke the 
`kedro-glass` functions. For a recommended implementation, see below. 

In `hooks.py`:
```python
from typing import Any, Dict, Iterable, Optional

from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal

from kedro_glass.core import (
    get_connector_conf_paths,
    get_glass_pipelines,
)
from pathlib import Path
from kedro.pipeline import Pipeline


class ProjectHooks:

    @hook_impl
    def register_pipelines(self) -> Dict[str, Pipeline]:
        "Registers the pipelines with the KedroContext using glass."
        return get_glass_pipelines(self.config_loader)

    @hook_impl
    def register_config_loader(
        self, conf_paths: Iterable[str], env: str, extra_params: Dict[str, Any]
    ) -> ConfigLoader:
        project_path = Path(__file__).parents[2]
        conf_paths = get_connector_conf_paths(
            conf_paths=conf_paths, project_path=project_path, ensure_local=True
        )

        self.config_loader = ConfigLoader(conf_paths)

        return self.config_loader

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ) -> DataCatalog:
        return DataCatalog.from_config(
            catalog, credentials, load_versions, save_version, journal
        )
```

#### Step 2. Configure your nodes

Nodes are functions that manipulate data. Nodes can be defined in a `nodes.yml` (or 
any yml file starting with `nodes`) in any of the relevant configuration areas 
(e.g. `conf/base`, `conf/local`, `conf/<env>`). In this situation, the item's key 
in the yaml file is the node name and the remaining metadata can be specified, 
including a package reference to the callable to be used. 

The input arguments in nodes correspond to catalog items with the same name at runtime.

In order for a node to be valid, it simply needs the appropriate prefix.

Nodes may have the following attributes:
- inputs: a series of inputs to be supplied (default: the input argument names)
- outputs: a series of outputs to be supplied (default: the node name without the prefix (e.g. `node_`))
- name: the unique name of the node (default: the node name without the prefix (e.g. `node_`))
- tags: a list of strings to associate with the node (default: `[]`)

Example: 
```yaml
int_customers:
  func: src.nodes.node_deduplicated
  inputs:
    - raw_customers
  outputs:
    - int_customers
  tags:
     - data_cleaning
``` 
 

#### Step 3. Configure your `pipelines.yml` file

Glass defines analytics processes as pipelines and nodes. Nodes belong to 
pipelines which list them (and other pipelines) by name. 

Within your configuration directories (usually `conf/base` and `conf/local`), 
ensure you have written your `pipeline.yml` files. These files should be 
relevant to the environment. E.g. pipelines.yml files in `conf/base` should 
be expected to be used by everyone, files in `conf/local` should be expected 
to be used for only local users (e.g. a developer in sandbox mode), etc. 

Pipelines can be as small as a single node or consist of multiple applications 
joined together. 

Pipelines can also be detailed in a `pipelines.yml` file (or any yml file starting 
with `pipelines`) located in the configuration directory (`conf` by default). 

Pipelines defined in a `pipelines.yml` file can reference nodes and/or other pipelines 
to include in them. 

```yaml
core:
  nodes:
    - deduplicated
    - renamed_columns
    - primary_table
    - model_input

validation:
  nodes: 
    - validate_model_input
  pipelines:
    - core
```

In this example, the `core` pipeline consists of the nodes `process_1`, `collect_raw`, 
`process_primary`, `model_input`. The 
`validation` pipeline consists of the `core` pipeline and a node `validate_model_input`. 

Note that the arguments for kedro modular pipelines may optionally be included as 
dictionary arguments. These include namespaces, inputs, outputs, parameters. 
For more information on the these options see 
https://kedro.readthedocs.io/en/stable/04_user_guide/06_pipelines.html#connecting-existing-pipelines



#### Step 3a (optional). Create the relevant configuration in the connectors style

You can now begin to develop using a connector structure. Given the connector glob 
above will search the project's src directory for a sub-directory called 
`connectors`, you must create a connector subdirectory here. 

Within this, you should develop connectors that contain the desired level of reuse. 
For ETL and advanced analytics applications, experience has shown that connectors 
are best suited to situations where reuse is likely. For example:
 - Data-source cleaning and ingestion tasks: When a data source is commonly used in 
   an organisation, it can be valuable to have a prepared connector which cleans 
   and ingests this data. An example might be a connector which ingests public 
   health information from the WHO.
 - Data model / feature generation tasks: When data from a variety of sources can 
   support a data model, it can be helpful to build primary data model or feature 
   generation connectors. An example might be a connector which takes processed 
   CRM data and creates salesforce features for a model.
 - Modelling or optimisation tasks: When/if your organisation has conventions in 
   how they parameterise and execute their modelling and optimisation, it can be 
   helpful to encapsulate common approaches into connectors. 

When creating a connector, it is best to follow a standard structure. 
The recommneded structure contains a directory for source code, configuration, 
and tests. For example, implementing the connectors mentioned above, the 
following structure would be used. 

```

   src
    ├── requirements.txt
    |
    └-- my_project
        ├── run.py
        └── connectors
            ├── who_collector
            |    ├── conf
            |    |     ├── catalog.yml
            |    |     ├── parameters.yml
            |    |     ├── pipelines.yml
            |    |     └── nodes.yml
            |    ├── src
            |    |    └── ingest_who_data.py
            |    └── tests
            |         └── test_nodes.py
            ├── crm_feature_builder
            |    ├── conf
            |    |     ├── catalog.yml
            |    |     ├── parameters.yml
            |    |     ├── pipelines.yml
            |    |     └── nodes.yml
            |    ├── src
            |    |    └── build_crm_features.py
            |    └── tests
            |         └── test_nodes.py
            └── modeling
                 ├── conf
                 |     ├── catalog.yml
                 |     ├── parameters.yml
                 |     ├── pipelines.yml
                 |     └── nodes.yml
                 ├── src
                 |    └── modelling.py
                 └── tests
                      └── test_nodes.py
```

#### Step 4. Running a pipeline

To run a pipeline, simply use the `kedro run` command. Note that you must specify 
which pipeline to run if you haven't configured a default pipeline. 

For example, to run the pipeline `core` simply execute `kedro run --pipeline=core` 
from project root.

If you would like to have a default pipeline, you will have to define a new pipeline 
named `default`. Considering the previous example, you can make the `core` pipeline as 
default by creating a new pipeline named `default` that consists of the `core` 
pipeline as per the example bellow.
With this configuration a simple `kedro run` will execute the `core` pipeline.

```yaml
core:
  nodes:
    - deduplicated
    - renamed_columns
    - primary_table
    - model_input

default:
  pipelines:
    - core
```


## Does kedro glass support global configuration variables and how do these work?

Yes. 

In some situations, regularly recurring details in configuration 
(e.g. a data store location) may be replaced with references to global 
configuration variables. For situations where frequently repeating text snippets 
or configuration variables may change or be replaced, this pattern can greatly 
speed refactoring and maintenance tasks. 

This functionality is provided by Kedro's `TemplatedConfigLoader` 
(found in [kedro.config.TemplatedConfigLoader](https://kedro.readthedocs.io/en/stable/kedro.config.TemplatedConfigLoader.html)). 
This is alternative to Kedro's standard `ConfigLoader`. References to global 
onfiguration variables are wrapped in brackets like: `${...}`, and the definition 
of these global configuration variables is stored in `globals.yml` in relevant 
configuration directories. 

To implement the `TemplatedConfigLoader`, ensure that the `register_config_loader` 
method in ProjectHooks is returning a `TemplatedConfigLoader`.



## Where did kedro glass come from and who is looking after it now?

`kedro-glass` is the product of many iterations and experiments in the architecture of 
advanced analytics engagements and was developed 
by [Roman Drapeko](https://github.com/drqb) and [Will Ashford](https://github.com/willashford). 

`kedro-glass` currently is owned and actively being maintained by PMPx, a part of
[QuantumBlack Labs](https://www.quantumblack.com/labs/).
