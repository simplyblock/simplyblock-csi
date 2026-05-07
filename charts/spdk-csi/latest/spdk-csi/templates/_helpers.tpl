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
Volume named "tls" holding only ca.crt, for client-only consumers without mTLS.
- openshift: cabundle ConfigMap (renamed key).
- cert-manager: project ca.crt out of simplyblock-ca-bundle-tls, a Certificate
  issued by the chart's ClusterIssuer into the release namespace. The root CA
  secret lives in the cert-manager namespace and cannot be mounted directly.
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
    # simplyblock-ca-bundle-tls is issued in the release namespace by the chart's
    # ClusterIssuer, so pods can mount it. The CA secret itself lives in the
    # cert-manager namespace and is not directly mountable from here.
    secretName: simplyblock-ca-bundle-tls
    items:
    - key: ca.crt
      path: ca.crt
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Volume named "tls" for client pods, with optional mTLS.
Args: dict "ctx" $root "clientSecret" <client-cert-secret-name>
- mutual_enabled=false: delegate to simplyblock.caVolume (CA-only mount).
- mutual_enabled=true: project the client cert Secret alongside the
  per-provider CA bundle.
Caller pipes through `nindent N`.
*/}}
{{- define "simplyblock.clientTlsVolume" -}}
{{- $ctx := .ctx -}}
{{- $clientSecret := .clientSecret -}}
{{- if $ctx.Values.tls.enabled -}}
{{- if not $ctx.Values.tls.mutual_enabled -}}
{{- include "simplyblock.caVolume" $ctx }}
{{- else if eq $ctx.Values.tls.provider "openshift" }}
- name: tls
  projected:
    sources:
    - secret:
        name: {{ $clientSecret }}
    - configMap:
        name: simplyblock-certificate-authority
        items:
        - key: service-ca.crt
          path: ca.crt
{{- else if eq $ctx.Values.tls.provider "cert-manager" }}
- name: tls
  secret:
    # cert-manager populates the client cert secret with tls.crt, tls.key, and
    # ca.crt, so a projected volume is not needed.
    secretName: {{ $clientSecret }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Shared TLS volumeMount entry for the "tls" volume, gated on tls.enabled.
Caller pipes through `nindent N` to land it inside a `volumeMounts:` list.
*/}}
{{- define "simplyblock.tlsVolumeMount" -}}
{{- if .Values.tls.enabled }}
- name: tls
  mountPath: /etc/simplyblock/tls
  readOnly: true
{{- end }}
{{- end -}}

{{/*
TLS-related env vars for sbcli containers. Caller pipes through `nindent N`
to land them at the right column inside an `env:` list.
*/}}
{{- define "simplyblock.tlsEnv" -}}
{{- if .Values.tls.enabled }}
- name: SB_TLS_SERVE
  value: "1"
- name: SB_TLS_PROVIDER
  value: {{ .Values.tls.provider | quote }}
{{- end }}
{{- if .Values.tls.mutual_enabled }}
- name: SB_TLS_CLIENT_AUTH
  value: "required"
- name: SB_TLS_CONNECT
  value: "authenticated"
{{- else if .Values.tls.enabled }}
- name: SB_TLS_CONNECT
  value: "anonymous"
{{- end }}
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
  {{- include "simplyblock.tlsVolumeMount" . | nindent 2 }}

resources:
  requests:
    cpu: "50m"
    memory: "100Mi"
  limits:
    cpu: "300m"
    memory: "1Gi"
{{- end }}
