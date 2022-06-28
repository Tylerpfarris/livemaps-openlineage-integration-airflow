import os
import inspect
import logging
from typing import Optional, List, Callable, Dict

from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.facets import UnknownOperatorAttributeRunFacet, UnknownOperatorInstance
from openlineage.client.facet import SourceCodeJobFacet
from openlineage.client.run import Dataset


log = logging.getLogger(__name__)


class PythonExtractor(BaseExtractor):
    """
    This extractor provides visibility on what particular task does by extracting
    executed source code and putting it into SourceCodeJobFacet. It does not extract
    datasets.
    """
    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["PythonOperator", "_PythonDecoratedOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        log.info(self)
        log.info(self.operator.__dict__.items())
        collect_source = True
        if os.environ.get(
            "OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "False"
        ).lower() in ('true', '1', 't'):
            collect_source = False

        source_code = self.get_source_code(self.operator.python_callable)
        job_facet: Dict = {}
        if collect_source and source_code:
            job_facet = {
                "sourceCode": SourceCodeJobFacet(
                    "python",
                    # We're on worker and should have access to DAG files
                    source_code
                )
            }
        
        collect_manual_lineage = False
        if os.environ.get(
            "OPENLINEAGE_COLLECT_MANUALLY", "False"
        ).lower() in ('true', '1', 't'):
            collect_manual_lineage = True
        
        _inputs: List = self.operator.get_inlet_defs() or None
        _outputs: List = self.operator.get_outlet_defs() or None
        
        input_properties: Dict = {}

        for x in self.operator.__dict__.items():
            if 'inlets' in x:
                input_properties['inputs']=x
            if 'task_id' in x:
                input_properties['task_id'] = x
        
        log.info(input_properties)
        #_outputs: Dict = {}
        if collect_manual_lineage:
            #_inputs = self.operator.get_inlet_defs()
            log.info("ENV WORKED~~~")
            #_inputs ={attr: value
            #        for attr, value in self.operator.get_inlet_defs()}
        #    _outputs ={attr: value
        #            for attr, value in self.operator.get_outlet_defs()}

        log.info(_inputs)
        log.info(_outputs)

        return TaskMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            job_facets=job_facet,
            run_facets={

                # The BashOperator is recorded as an "unknownSource" even though we have an
                # extractor, as the <i>data lineage</i> cannot be determined from the operator
                # directly.
                "unknownSourceAttribute": UnknownOperatorAttributeRunFacet(
                    unknownItems=[
                        UnknownOperatorInstance(
                            name="PythonOperator",
                            properties={attr: value
                                        for attr, value in self.operator.__dict__.items()}
                        )
                    ]
                )
            },
            #outputs=_outputs or None,
            inputs=Dataset(namespace=_inputs[0]["database"],name=_inputs[0]["name"]),
            outputs=Dataset(namespace=_outputs[0]["database"],name=_outputs[0]["name"])
            
        )

    def get_source_code(self, callable: Callable) -> Optional[str]:
        try:
            return inspect.getsource(callable)
        except TypeError:
            # Trying to extract source code of builtin_function_or_method
            return str(callable)
        except OSError:
            log.exception(f"Can't get source code facet of PythonOperator {self.operator.task_id}")
        return None
