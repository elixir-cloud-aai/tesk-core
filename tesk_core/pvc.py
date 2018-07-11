from uuid import uuid4
from kubernetes import client


class PVC(object):
    _k8s_core = client.CoreV1Api()

    def __init__(self, paths, basename, name='task-pvc', size_gb=1, namespace='default'):
        self.name = name
        self.spec = {'apiVersion': 'v1',
                     'kind': 'PersistentVolumeClaim',
                     'metadata': {'name': name},
                     'spec': {
                         'accessModes': ['ReadWriteOnce'],
                         'resources': {'requests': {'storage': str(size_gb) + 'Gi'}},
                     }
                    }

        self.namespace = namespace
        self.basename = basename
        self.volume_mounts = self._generate_mounts(paths)

        self._k8s_core.create_namespaced_persistent_volume_claim(
            self.namespace, self.spec)

    def delete(self):
        self._k8s_core.delete_namespaced_persistent_volume_claim(
            self.name, self.namespace, client.V1DeleteOptions())

    def volume(self):
        return {'name': self.basename,
                'persistentVolumeClaim': {
                    'readonly': False, 'claimName': self.name
                }
               }

    def _generate_mounts(self, paths):
        # It's not clear whether we should ignore if there are two mounts with the
        # same mount path or throw some kind of error.
        # We use uuid to generate unique subPaths
        volume_mounts = {
            path: {'name': self.basename, 'mountPath': path, 'subPath': uuid4()}
            for path in paths}

        return list(volume_mounts.values())
