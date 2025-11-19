from pydantic import BaseModel, Field
from krkn_ai.utils.rng import rng
from krkn_ai.models.scenario.base import BaseParameter

class DummyEndParameter(BaseParameter):
    krknhub_name: str = "END"
    krknctl_name: str = "end"
    value: int = 10


class DummyExitStatusParameter(BaseParameter):
    krknhub_name: str = "EXIT_STATUS"
    krknctl_name: str = "exit-status"
    value: int = 0


class NamespaceParameter(BaseParameter):
    krknhub_name: str = "NAMESPACE"
    krknctl_name: str = "namespace"
    value: str = ""


class PodLabelParameter(BaseParameter):
    krknhub_name: str = "POD_LABEL"
    krknctl_name: str = "pod-label"
    value: str = ""  # Example: service=payment


class NamePatternParameter(BaseParameter):
    krknhub_name: str = "NAME_PATTERN"
    krknctl_name: str = "name-pattern"
    value: str = ".*"


class DisruptionCountParameter(BaseParameter):
    krknhub_name: str = "DISRUPTION_COUNT"
    krknctl_name: str = "disruption-count"
    value: int = 1


class KillTimeoutParameter(BaseParameter):
    krknhub_name: str = "KILL_TIMEOUT"
    krknctl_name: str = "kill-timeout"
    value: int = 60


class ExpRecoveryTimeParameter(BaseParameter):
    krknhub_name: str = "EXPECTED_RECOVERY_TIME"
    krknctl_name: str = "expected-recovery-time"
    value: int = 60



class DurationParameter(BaseParameter):
    krknhub_name: str = "DURATION"
    krknctl_name: str = "chaos-duration"
    value: int = 60



class PodSelectorParameter(BaseParameter):
    krknhub_name: str = "POD_SELECTOR"
    krknctl_name: str = "pod-selector"
    value: str = "" # Format: {app: foo}


class BlockTrafficType(BaseParameter):
    krknhub_name: str = "BLOCK_TRAFFIC_TYPE"
    krknctl_name: str = "block-traffic-type"
    value: str = "[Ingress, Egress]" # "[Ingress, Egress]", "[Ingress]", "[Egress]"


class LabelSelectorParameter(BaseParameter):
    krknhub_name: str = "LABEL_SELECTOR"
    krknctl_name: str = "label-selector"
    value: str = "" # Example Value: k8s-app=etcd


class ContainerNameParameter(BaseParameter):
    krknhub_name: str = "CONTAINER_NAME"
    krknctl_name: str = "container-name"
    value: str = ""  # Example Value: etcd

class ActionParameter(BaseParameter):
    krknhub_name: str = "ACTION"
    krknctl_name: str = "action"
    value: str = "1"
    # possible_values = ["1", "9"]


class TotalChaosDurationParameter(BaseParameter):
    krknhub_name: str = "TOTAL_CHAOS_DURATION"
    krknctl_name: str = "chaos-duration"
    value: int = 60


class NodeCPUCoreParameter(BaseParameter):
    krknhub_name: str = "NODE_CPU_CORE"
    krknctl_name: str = "cores"
    value: float = 2


class NodeCPUPercentageParameter(BaseParameter):
    '''
    CPU usage percentage of the node cpu hog scenario between 20 and 100.
    '''
    krknhub_name: str = "NODE_CPU_PERCENTAGE"
    krknctl_name: str = "cpu-percentage"
    value: int = 50

    def mutate(self):
        if rng.random() < 0.5:
            self.value += rng.randint(1, 35) * self.value / 100
        else:
            self.value -= rng.randint(1, 25) * self.value / 100
        self.value = int(self.value)
        self.value = max(self.value, 20)
        self.value = min(self.value, 100)

class NodeMemoryPercentageParameter(BaseParameter):
    '''
    Memory usage percentage of the node memory hog scenario between 20 and 100.
    '''
    krknhub_name: str = "MEMORY_CONSUMPTION_PERCENTAGE"
    krknctl_name: str = "memory-consumption"
    value: int = 50

    def get_value(self):
        return f"{self.value}%"

    def mutate(self):
        if rng.random() < 0.5:
            self.value += rng.randint(1, 35) * self.value / 100
        else:
            self.value -= rng.randint(1, 25) * self.value / 100
        self.value = int(self.value)
        self.value = max(self.value, 20)
        self.value = min(self.value, 100)


class NumberOfWorkersParameter(BaseParameter):
    krknhub_name: str = "NUMBER_OF_WORKERS"
    krknctl_name: str = "memory-workers"
    value: int = 1

    def mutate(self):
        self.value = rng.randint(1, 10)


class NodeSelectorParameter(BaseParameter):
    '''
    CPU-Hog:
    Node selector where the scenario containers will be scheduled in the format “=<selector>”. 
    NOTE: Will be instantiated a container per each node selected with the same scenario options. 
    If left empty a random node will be selected	

    Memory-Hog:
    defines the node selector for choosing target nodes. If not specified, one schedulable node in the cluster will be chosen at random. If multiple nodes match the selector, all of them will be subjected to stress. If number-of-nodes is specified, that many nodes will be randomly selected from those identified by the selector.	
    '''
    krknhub_name: str = "NODE_SELECTOR"
    krknctl_name: str = "node-selector"
    value: str = ""


class TaintParameter(BaseParameter):
    krknhub_name: str = "TAINTS"
    krknctl_name: str = "taints"
    value: str = '[]'


class NumberOfNodesParameter(BaseParameter):
    krknhub_name: str = "NUMBER_OF_NODES"
    krknctl_name: str = "number-of-nodes"
    value: int = 1


class HogScenarioImageParameter(BaseParameter):
    krknhub_name: str = "IMAGE"
    krknctl_name: str = "image"
    value: str = "quay.io/krkn-chaos/krkn-hog"

class ObjectTypeParameter(BaseParameter):
    krknhub_name: str = "OBJECT_TYPE"
    krknctl_name: str = "object-type"
    value: str = ""  # Available Types: pod, node

    def mutate(self):
        self.value = rng.choice(["pod", "node"])
 
class ActionTimeParameter(BaseParameter):
    krknhub_name: str = "ACTION"
    krknctl_name: str = "action"
    value: str = "skew_date" # Available Types: skew_date, skew_time

    def mutate(self):
        self.value = rng.choice(["skew_date", "skew_time"])


class NetworkScenarioTypeParameter(BaseParameter):
    krknhub_name: str = "NETWORK_SCENARIO_TYPE"
    krknctl_name: str = "traffic-type"
    value: str = "ingress"

    def mutate(self):
        self.value = rng.choice(["ingress", "egress"])

class NetworkScenarioImageParameter(BaseParameter):
    krknhub_name: str = "IMAGE"
    krknctl_name: str = "image"
    value: str = "quay.io/krkn-chaos/krkn:tools"

class NetworkScenarioDurationParameter(BaseParameter):
    krknhub_name: str = "DURATION"
    krknctl_name: str = "duration"
    value: int = 120

class NetworkScenarioLabelSelectorParameter(BaseParameter):
    krknhub_name: str = "LABEL_SELECTOR"
    krknctl_name: str = "label-selector"
    value: str = ""

class NetworkScenarioExecutionParameter(BaseParameter):
    krknhub_name: str = "EXECUTION"
    krknctl_name: str = "execution"
    value: str = "parallel"

    def mutate(self):
        self.value = rng.choice(["serial", "parallel"])

class NetworkScenarioNodeNameParameter(BaseParameter):
    krknhub_name: str = "NODE_NAME"
    krknctl_name: str = "node-name"
    value: str = ""

class NetworkScenarioInterfacesParameter(BaseParameter):
    # TODO: Understand the format and values of the interfaces parameter
    krknhub_name: str = "INTERFACES"
    krknctl_name: str = "interfaces"
    value: str = "[]"

class NetworkParamData(BaseModel):
    latency: int = 50 # ms
    loss: float = 0.02  # %
    bandwidth: int = 100 # mbit


class NetworkScenarioNetworkParamsParameter(BaseParameter):
    krknhub_name: str = "NETWORK_PARAMS"
    krknctl_name: str = "network-params"
    value: NetworkParamData = Field(default_factory=NetworkParamData)

    def mutate(self):
        self.value.latency = rng.randint(1, 1000)
        self.value.loss = round(rng.uniform(0.01, 0.1), 2)
        self.value.bandwidth = rng.randint(100, 1000)

    def get_value(self):
        return "{" + f"latency: {self.value.latency}ms,loss: {self.value.loss},bandwidth: {self.value.bandwidth}mbit" + "}"

class NetworkScenarioEgressParamsParameter(BaseParameter):
    krknhub_name: str = "EGRESS"
    krknctl_name: str = "egress"
    value: NetworkParamData = Field(default_factory=NetworkParamData)

    def mutate(self):
        self.value.latency = rng.randint(1, 1000)
        self.value.loss = round(rng.uniform(0.01, 0.1), 2)
        self.value.bandwidth = rng.randint(100, 1000)

    def get_value(self):
        return "{" + f"latency: {self.value.latency}ms,loss: {self.value.loss},bandwidth: {self.value.bandwidth}mbit" + "}"

class NetworkScenarioTargetNodeInterfaceParameter(BaseParameter):
    # TODO: Understand the format and values of the target-node-interface parameter
    krknhub_name: str = "TARGET_NODE_AND_INTERFACE"
    krknctl_name: str = "target-node-interface"
    value: str = "{}"

class DNSOutageDurationParameter(BaseParameter):
    krknhub_name: str = "TEST_DURATION"
    krknctl_name: str = "chaos-duration"
    value: int = 60

class DNSOutageProtocolParameter(BaseParameter):
    krknhub_name: str = "PROTOCOL"
    krknctl_name: str = "protocol"
    value: str = "tcp,udp"

class DNSPortParameter(BaseParameter):
    krknhub_name: str = "PORTS"
    krknctl_name: str = "ports"
    value: str = ""


class PodNameParameter(BaseParameter):
    krknhub_name: str = "POD_NAME"
    krknctl_name: str = "pod-name"
    value: str = ""

class IngressParameter(BaseParameter):
    krknhub_name: str = "INGRESS"
    krknctl_name: str = "ingress"
    value: str = "false"

class EgressParameter(BaseParameter):
    krknhub_name: str = "EGRESS"
    krknctl_name: str = "egress"
    value: str = "true"

