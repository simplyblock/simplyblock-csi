{{/* vim: set filetype=mustache: */}}

{{/* labels for helm resources */}}
{{- define "spdk.labels" -}}
labels:
  heritage: "{{ .Release.Service }}"
  release: "{{ .Release.Name }}"
  revision: "{{ .Release.Revision }}"
  chart: "{{ .Chart.Name }}"
  chartVersion: "{{ .Chart.Version }}"
{{- end -}}

{{- define "simplyblock.commonContainer" }}
env:
  - name: SIMPLYBLOCK_LOG_LEVEL
    valueFrom:
      configMapKeyRef:
        name: simplyblock-config
        key: LOG_LEVEL

volumeMounts:
  - name: fdb-cluster-file
    mountPath: /etc/foundationdb/fdb.cluster
    subPath: fdb.cluster

resources:
  requests:
    cpu: "50m"
    memory: "100Mi"
  limits:
    cpu: "300m"
    memory: "1Gi"
{{- end }}
