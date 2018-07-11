from kubernetes.client import V1DeleteOptions
from tesk_core.pvc import PVC

import pytest

@pytest.fixture
def default_pvc(mocker):
    mocker.spy(PVC, '_k8s_core')
    return PVC([], 'foo')

def test_attributes(default_pvc):
    assert default_pvc.name == 'task-pvc'
    assert default_pvc.namespace == 'default'
    assert default_pvc.basename == 'foo'
    assert default_pvc.volume_mounts == []
    assert default_pvc.spec['metadata']['name'] == default_pvc.name
    assert default_pvc.spec['spec']['resources']['requests']['storage'] == '1Gi'

def test_api(default_pvc):
    volume = default_pvc.volume()
    assert volume['name'] == 'foo'
    assert volume['persistentVolumeClaim']['claimName'] == 'task-pvc'

    # check side-effects of deleting the pvc
    default_pvc.delete()
    default_pvc._k8s_core.delete_namespaced_persistent_volume_claim.assert_called_once_with(
        default_pvc.name,
        default_pvc.namespace,
        V1DeleteOptions()
    )

def test_mount_generation(default_pvc):
    # confirm that uuids are actually unique
    paths = [str(k) for k in range(100000)]
    mounts = default_pvc._generate_mounts(paths)

    unique_subpaths = set(mount['subPath'] for mount in mounts)
    assert len(unique_subpaths) == len(mounts)
