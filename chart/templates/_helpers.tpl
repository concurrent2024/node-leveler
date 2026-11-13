{{/* Object names default to node-leveler; override with nameOverride / fullnameOverride when running several instances. */}}
{{- define "node-leveler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "node-leveler.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "node-leveler.name" . -}}
{{- end -}}
{{- end -}}

{{- define "node-leveler.namespace" -}}
{{- default .Release.Namespace .Values.namespace -}}
{{- end -}}

{{- define "node-leveler.labels" -}}
app.kubernetes.io/name: {{ include "node-leveler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* State ConfigMap name (set in config.STATE_CM, created and owned by the balancer itself) */}}
{{- define "node-leveler.stateConfigMap" -}}
{{- default "node-leveler-state" .Values.stateConfigMapName -}}
{{- end -}}
