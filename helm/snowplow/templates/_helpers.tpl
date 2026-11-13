{{/*
Expand the name of the chart.
*/}}
{{- define "snowplow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "snowplow.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "snowplow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "snowplow.labels" -}}
helm.sh/chart: {{ include "snowplow.chart" . }}
{{ include "snowplow.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "snowplow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "snowplow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "snowplow.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "snowplow.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
#1334 — "true" when the install opted in to the public identity, else "false".
The ONE reading of publicIdentity.enabled: the ConfigMap's PUBLIC_IDENTITY_ENABLED
and the ClusterRole's impersonate grant both use it, so they cannot disagree.
Only a boolean true opts in.
*/}}
{{- define "snowplow.publicIdentityEnabled" -}}
{{- $p := .Values.publicIdentity | default dict -}}
{{- if eq (get $p "enabled" | toString) "true" -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{/*
Image reference honoring an optional global.imageRegistry override (host-only; repository paths
preserved). Canonical helper — identical across all Krateo charts.
*/}}
{{- define "krateo.image" -}}
{{- $g := .global | default dict -}}
{{- $registry := $g.imageRegistry | default .img.registry -}}
{{- $tag := .img.tag | default .defaultTag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .img.repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .img.repository $tag -}}
{{- end -}}
{{- end -}}
