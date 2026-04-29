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

{{- define "simplyblock.controlPlaneAddr" -}}
{{- if .Values.csiConfig.simplybk.ip -}}
{{ .Values.csiConfig.simplybk.ip }}
{{- else if .Values.operator.enabled -}}
http://simplyblock-webappapi.{{ .Release.Namespace }}.svc.cluster.local:5000
{{- end -}}
{{- end -}}

{{/*
Volume named "tls" holding the serving cert bundle for pods that terminate TLS.
Args: dict "ctx" $root "secret" <serving-cert-secret-name>
- openshift: project the serving Secret with the cabundle ConfigMap (renaming
  service-ca.crt -> ca.crt) since the Secret carries only tls.crt/tls.key.
- cert-manager: mount the Secret directly; it already contains ca.crt.
Caller pipes through `nindent N`.
*/}}
{{- define "simplyblock.tlsVolume" -}}
{{- $ctx := .ctx -}}
{{- $secret := .secret -}}
{{- if $ctx.Values.tls.enabled -}}
{{- if eq $ctx.Values.tls.provider "openshift" }}
- name: tls
  projected:
    sources:
    - secret:
        name: {{ $secret }}
    - configMap:
        name: simplyblock-certificate-authority
        items:
        - key: service-ca.crt
          path: ca.crt
{{- else if eq $ctx.Values.tls.provider "cert-manager" }}
- name: tls
  secret:
    secretName: {{ $secret }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Volume named "tls" holding only ca.crt, for consumer-only pods (operator).
- openshift: cabundle ConfigMap (renamed key).
- cert-manager: project ca.crt out of simplyblock-webappapi-tls — same Issuer
  CA backs every workload, and that Secret is owned by the chart.
Caller pipes through `nindent N`.
*/}}
{{- define "simplyblock.caVolume" -}}
{{- if .Values.tls.enabled -}}
{{- if eq .Values.tls.provider "openshift" }}
- name: tls
  configMap:
    name: simplyblock-certificate-authority
    items:
    - key: service-ca.crt
      path: ca.crt
{{- else if eq .Values.tls.provider "cert-manager" }}
- name: tls
  secret:
    secretName: simplyblock-webappapi-tls
    items:
    - key: ca.crt
      path: ca.crt
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
TLS-related env vars for sbcli containers. Caller pipes through `nindent N`
to land them at the right column inside an `env:` list.
*/}}
{{- define "simplyblock.tlsEnv" -}}
- name: SB_TLS_SERVE
  value: {{ .Values.tls.enabled | quote }}
- name: SB_TLS_PROVIDER
  value: {{ .Values.tls.provider | quote }}
- name: SB_TLS_CONNECT
  value: {{ ternary "anonymous" "disabled" .Values.tls.enabled | quote }}
{{- end -}}

{{- define "simplyblock.commonContainer" }}
env:
  - name: SIMPLYBLOCK_LOG_LEVEL
    valueFrom:
      configMapKeyRef:
        name: simplyblock-config
        key: LOG_LEVEL
  {{- include "simplyblock.tlsEnv" . | nindent 2 }}

volumeMounts:
  - name: fdb-cluster-file
    mountPath: /etc/foundationdb/fdb.cluster
    subPath: fdb.cluster
{{- if .Values.tls.enabled }}
  - name: tls
    mountPath: /etc/simplyblock/tls
    readOnly: true
{{- end }}

resources:
  requests:
    cpu: "50m"
    memory: "100Mi"
  limits:
    cpu: "300m"
    memory: "1Gi"
{{- end }}
