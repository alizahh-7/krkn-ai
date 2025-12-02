from typing import List, Tuple, Optional
from krkn_ai.models.custom_errors import ScenarioParameterInitError
from krkn_ai.utils.rng import rng
from krkn_ai.models.scenario.base import Scenario
from krkn_ai.models.scenario.parameters import *
from krkn_ai.models.cluster_components import Namespace, Pod, PVC
from krkn_ai.utils.pvc_utils import get_pvc_usage_percentage
from krkn_ai.utils.logger import get_logger

logger = get_logger(__name__)


class PVCScenario(Scenario):
    name: str = "pvc-scenarios"
    krknctl_name: str = "pvc-scenarios"
    krknhub_image: str = "quay.io/krkn-chaos/krkn-hub:pvc-scenarios"

    namespace: NamespaceParameter = NamespaceParameter()
    pvc_name: PVCNameParameter = PVCNameParameter()
    pod_name: PodNameParameter = PodNameParameter()
    fill_percentage: FillPercentageParameter = FillPercentageParameter()
    duration: StandardDurationParameter = StandardDurationParameter(value=20)

    def __init__(self, **data):
        super().__init__(**data)
        self.mutate()

    @property
    def parameters(self):
        # pvc-name and pod-name are mutually exclusive, at least one is required
        params = [
            self.namespace,
            self.fill_percentage,
            self.duration,
        ]
        if self.pvc_name.value:
            params.insert(1, self.pvc_name)
        elif self.pod_name.value:
            params.insert(1, self.pod_name)
        return params

    def mutate(self):
        if len(self._cluster_components.namespaces) == 0:
            raise ScenarioParameterInitError("No namespaces found in cluster components")
        
        namespace_pvc_tuple: List[Tuple[Namespace, PVC]] = []  # (namespace, pvc)
        namespace_pod_tuple: List[Tuple[Namespace, Pod]] = []  # (namespace, pod)
        
        for namespace in self._cluster_components.namespaces:
            # Prefer PVCs if available
            if namespace.pvcs and len(namespace.pvcs) > 0:
                for pvc in namespace.pvcs:
                    namespace_pvc_tuple.append((namespace, pvc))
            # Fallback to pods
            elif namespace.pods and len(namespace.pods) > 0:
                for pod in namespace.pods:
                    namespace_pod_tuple.append((namespace, pod))
        
        # Prefer PVCs
        selected_pvc_name = None
        selected_namespace = None
        if len(namespace_pvc_tuple) > 0:
            namespace, pvc = rng.choice(namespace_pvc_tuple)
            self.namespace.value = namespace.name
            self.pvc_name.value = pvc.name
            self.pod_name.value = ""  # Leave empty when using pvc-name
            selected_pvc_name = pvc.name
            selected_namespace = namespace.name
        elif len(namespace_pod_tuple) > 0:
            # Fallback to pods
            namespace, pod = rng.choice(namespace_pod_tuple)
            self.namespace.value = namespace.name
            self.pod_name.value = pod.name
            self.pvc_name.value = ""  # Leave empty when using pod-name
            selected_pvc_name = None
        else:
            raise ScenarioParameterInitError("No PVCs or pods found in cluster components for PVC scenario")
        
        min_usage = None
        if selected_pvc_name:
            try:
                current_usage = get_pvc_usage_percentage(
                    pvc_name=selected_pvc_name,
                    namespace=selected_namespace
                )
                if current_usage is not None:
                    min_usage = current_usage
            except Exception as e:
                logger.debug("Failed to get real-time PVC usage for %s: %s", selected_pvc_name, str(e))
        
        self.fill_percentage.mutate(min_value=min_usage)

