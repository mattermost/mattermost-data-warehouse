from pathlib import Path

import pytest

from airflow import models as airflow_models
from airflow.utils.dag_cycle_tester import check_cycle

DAG_DIR = Path(__file__).parent.parent / "dags" / "mattermost_dags"
ALL_PATHS = set(DAG_DIR.rglob('*.py'))
EXCLUDED_PATHS = set(DAG_DIR.rglob('_*.py')).union({DAG_DIR / 'airflow_utils.py', DAG_DIR / 'kube_secrets.py'})

DAG_PATHS = ALL_PATHS - EXCLUDED_PATHS


@pytest.mark.parametrize("dag_path", [pytest.param(path, id=path.name) for path in DAG_PATHS])
def test_dag_integrity(dag_path):
    """Import DAG files and check for a valid DAG instance."""
    module = _import_file(dag_path.name, dag_path)
    # Validate if there is at least 1 DAG object in the file
    dag_objects = [var for var in vars(module).values() if isinstance(var, airflow_models.DAG)]
    assert dag_objects
    # For every DAG object, test for cycles
    for dag in dag_objects:
        check_cycle(dag)


def _import_file(module_name, module_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
