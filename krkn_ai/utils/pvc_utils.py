"""
Utilities for working with PVCs, including getting real-time usage percentage.
"""
from typing import Optional, Dict, Tuple
import time
from krkn_lib.k8s.krkn_kubernetes import KrknKubernetes
from kubernetes.stream import stream
from krkn_ai.utils.logger import get_logger
from krkn_ai.utils.cluster_manager import ClusterManager

logger = get_logger(__name__)

# Global kubeconfig path, set by initialize_kubeconfig()
_kubeconfig_path: Optional[str] = None

# Cache expires after 5 seconds to balance real-time accuracy and performance
_pvc_usage_cache: Dict[Tuple[str, str], Tuple[float, float]] = {}
_cache_ttl = 5.0  # Cache TTL in seconds
_logged_pvcs: set = set()


def initialize_kubeconfig(kubeconfig_path: str):
    """
    Initialize the kubeconfig path for PVC utilities.
    This should be called once at the start of the application.
    
    Args:
        kubeconfig_path: Path to kubeconfig file
    """
    global _kubeconfig_path
    _kubeconfig_path = kubeconfig_path


def get_pvc_usage_percentage(
    pvc_name: str,
    namespace: str,
    kubeconfig: Optional[str] = None
) -> Optional[float]:
    """
    Get current usage percentage of a PVC in real-time.
    Uses a short-term cache (5 seconds) to avoid excessive API calls when creating multiple scenarios.
    
    Args:
        pvc_name: Name of the PVC
        namespace: Namespace where the PVC exists
        kubeconfig: Optional path to kubeconfig file. If not provided, uses the globally initialized kubeconfig.
        
    Returns:
        Usage percentage (0-100) or None if unable to get
    """
    # Use provided kubeconfig or fall back to global one
    kubeconfig_path = kubeconfig or _kubeconfig_path
    if not kubeconfig_path:
        logger.debug("No kubeconfig provided and global kubeconfig not initialized")
        return None
    # Check cache first
    cache_key = (namespace, pvc_name)
    current_time = time.time()
    
    if cache_key in _pvc_usage_cache:
        cached_usage, cached_timestamp = _pvc_usage_cache[cache_key]
        if current_time - cached_timestamp < _cache_ttl:
            # Return cached value without logging to reduce log noise
            return cached_usage
        else:
            # Cache expired, remove it and allow logging again
            del _pvc_usage_cache[cache_key]
            _logged_pvcs.discard(cache_key)
    
    try:
        krkn_k8s = KrknKubernetes(kubeconfig_path=kubeconfig_path)
        core_api = krkn_k8s.cli
        
        # Get all pods in the namespace to find which pod uses this PVC
        # Using core_api directly to access raw Kubernetes pod objects with spec.volumes
        pods = core_api.list_namespaced_pod(namespace=namespace).items
        
        # Find a pod that uses this PVC
        target_pod = None
        target_volume = None
        
        for pod in pods:
            if pod.spec.volumes:
                for volume in pod.spec.volumes:
                    if volume.persistent_volume_claim and volume.persistent_volume_claim.claim_name == pvc_name:
                        target_pod = pod
                        target_volume = volume
                        break
                if target_pod:
                    break
        
        if not target_pod or not target_volume:
            logger.debug("No pod found using PVC %s in namespace %s", pvc_name, namespace)
            return None
        
        # Find the mount path for this PVC in the pod
        mount_path = None
        container_name = None
        for container in target_pod.spec.containers:
            if container.volume_mounts:
                for vm in container.volume_mounts:
                    if vm.name == target_volume.name:
                        mount_path = vm.mount_path
                        container_name = container.name
                        break
            if mount_path:
                break
        
        if not mount_path or not container_name:
            logger.debug("No mount path found for PVC %s in pod %s", pvc_name, target_pod.metadata.name)
            return None
        
        # Command: df <mount_path> -B 1024 | sed 1d
        command = f"df {mount_path} -B 1024 | sed 1d"
        exec_command = ['sh', '-c', command]
        
        resp = stream(
            core_api.connect_get_namespaced_pod_exec,
            target_pod.metadata.name,
            namespace,
            command=exec_command,
            container=container_name,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        
        if resp:
            # Format: Filesystem 1K-blocks Used Available Use% Mounted on
            command_output = resp.strip().split()
            
            if len(command_output) >= 4:
                try:
                    pvc_used_kb = int(command_output[2])
                    pvc_available_kb = int(command_output[3])
                    pvc_capacity_kb = pvc_used_kb + pvc_available_kb
                    
                    if pvc_capacity_kb > 0:
                        current_usage = (pvc_used_kb / pvc_capacity_kb) * 100
                        # Cache the result
                        _pvc_usage_cache[cache_key] = (current_usage, current_time)
                        # Only log once per PVC to reduce log noise
                        if cache_key not in _logged_pvcs:
                            logger.debug("Found PVC %s usage: %.2f%% (used: %d KB, capacity: %d KB)", 
                                        pvc_name, current_usage, pvc_used_kb, pvc_capacity_kb)
                            _logged_pvcs.add(cache_key)
                        return current_usage
                    else:
                        logger.debug("PVC %s capacity is 0, cannot calculate usage", pvc_name)
                except (ValueError, IndexError) as e:
                    logger.debug("Failed to parse df output for PVC %s: %s, output: %s", 
                                pvc_name, str(e), resp)
            else:
                logger.debug("Unexpected df output format for PVC %s: %s", pvc_name, resp)
        
        return None
    except Exception as e:
        logger.debug("Failed to get usage for PVC %s in namespace %s: %s", pvc_name, namespace, str(e))
        return None

