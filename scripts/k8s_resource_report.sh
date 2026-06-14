#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT_DIR}"

NAMESPACE="${NAMESPACE:-big-data-lab8}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-1}"
JOB_TIMEOUT="${JOB_TIMEOUT:-20m}"
RESOURCE_OUTPUT_DIR="${RESOURCE_OUTPUT_DIR:-outputs}"
RESOURCE_REPORT_CSV="${RESOURCE_REPORT_CSV:-${RESOURCE_OUTPUT_DIR}/k8s_resource_usage_$(date +%Y%m%d_%H%M%S).csv}"

# Fallback values used only when pod resource requests cannot be read.
SPARK_POD_CPU_M="${SPARK_POD_CPU_M:-1000}"
SPARK_POD_MEMORY_MI="${SPARK_POD_MEMORY_MI:-1024}"

mkdir -p "${RESOURCE_OUTPUT_DIR}"

log() {
  printf '[resource-report] %s\n' "$*"
}

check_metrics_api() {
  if ! kubectl top pods -n "${NAMESPACE}" >/dev/null 2>&1; then
    log "Kubernetes Metrics API is unavailable; continuing with direct cgroup sampling."
  fi
}

read_pod_cgroup_metrics() {
  local pod="$1"

  kubectl exec -n "${NAMESPACE}" "${pod}" -- sh -c '
    if [ -r /sys/fs/cgroup/cpu.stat ]; then
      cpu_usage_usec="$(sed -n "s/^usage_usec //p" /sys/fs/cgroup/cpu.stat)"
      cpu_usage_ns="$((cpu_usage_usec * 1000))"
    elif [ -r /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
      cpu_usage_ns="$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)"
    else
      exit 1
    fi

    if [ -r /sys/fs/cgroup/memory.current ]; then
      memory_bytes="$(cat /sys/fs/cgroup/memory.current)"
    elif [ -r /sys/fs/cgroup/memory/memory.usage_in_bytes ]; then
      memory_bytes="$(cat /sys/fs/cgroup/memory/memory.usage_in_bytes)"
    else
      exit 1
    fi

    printf "%s %s\n" "${cpu_usage_ns}" "${memory_bytes}"
  ' 2>/dev/null
}

read_pod_resource_allocations() {
  local pod="$1"
  local resources

  resources="$(
    kubectl get pod "${pod}" -n "${NAMESPACE}" \
      -o jsonpath='{.spec.containers[0].resources.requests.cpu}{" "}{.spec.containers[0].resources.requests.memory}' 2>/dev/null \
      || true
  )"

  awk \
    -v resources="${resources}" \
    -v default_cpu_m="${SPARK_POD_CPU_M}" \
    -v default_memory_mi="${SPARK_POD_MEMORY_MI}" '
      function cpu_to_millicores(value) {
        if (value == "") {
          return default_cpu_m
        }
        if (value ~ /m$/) {
          return substr(value, 1, length(value) - 1) + 0
        }
        return value * 1000
      }

      function memory_to_mib(value) {
        if (value == "") {
          return default_memory_mi
        }
        if (value ~ /Ki$/) {
          return substr(value, 1, length(value) - 2) / 1024
        }
        if (value ~ /Mi$/) {
          return substr(value, 1, length(value) - 2) + 0
        }
        if (value ~ /Gi$/) {
          return substr(value, 1, length(value) - 2) * 1024
        }
        if (value ~ /Ti$/) {
          return substr(value, 1, length(value) - 2) * 1048576
        }
        return value / 1048576
      }

      BEGIN {
        split(resources, fields, " ")
        printf "%.0f %.0f\n", cpu_to_millicores(fields[1]), memory_to_mib(fields[2])
      }
    '
}

find_spark_selector() {
  local spark_app_name="$1"

  kubectl get pods -n "${NAMESPACE}" \
    -l "spark-app-name=${spark_app_name},spark-role=driver" \
    -o jsonpath='{range .items[*]}{.metadata.labels.spark-app-selector}{"\n"}{end}' 2>/dev/null \
    | awk 'NF { print; exit }'
}

latest_driver_phase() {
  local spark_app_name="$1"

  kubectl get pods -n "${NAMESPACE}" \
    -l "spark-app-name=${spark_app_name},spark-role=driver" \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null \
    | awk 'NF { phase = $1 } END { print phase }'
}

list_running_spark_pods() {
  local spark_selector="$1"

  kubectl get pods -n "${NAMESPACE}" \
    -l "spark-app-selector=${spark_selector}" \
    --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.labels.spark-role}{"\n"}{end}' 2>/dev/null
}

now_ns() {
  python3 -c 'import time; print(time.time_ns())'
}

wait_for_job() {
  local job_name="$1"
  kubectl wait --for=condition=complete --timeout="${JOB_TIMEOUT}" "job/${job_name}" -n "${NAMESPACE}"
}

restart_job_without_measurement() {
  local manifest="$1"
  local job_name="$2"

  kubectl delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found
  kubectl apply -f "${manifest}"
  wait_for_job "${job_name}"
}

delete_old_spark_pods() {
  local spark_app_name="$1"
  local pods

  pods="$(
    kubectl get pods -n "${NAMESPACE}" --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null \
      | awk -v prefix="${spark_app_name}-" '$1 ~ "^" prefix { print $1 }'
  )"

  if [[ -n "${pods}" ]]; then
    while IFS= read -r pod; do
      [[ -z "${pod}" ]] && continue
      kubectl delete pod "${pod}" -n "${NAMESPACE}" --ignore-not-found --wait=false >/dev/null
    done <<< "${pods}"
  fi
}

monitor_spark_app() {
  local spark_app_name="$1"
  local csv_path="$2"
  local stop_file="$3"
  local spark_selector=""
  local previous_sample_file

  previous_sample_file="$(mktemp "${TMPDIR:-/tmp}/k8s-resource-report-samples.XXXXXX")"

  while [[ ! -f "${stop_file}" ]]; do
    local timestamp
    timestamp="$(date '+%Y-%m-%dT%H:%M:%S%z')"

    if [[ -z "${spark_selector}" ]]; then
      spark_selector="$(find_spark_selector "${spark_app_name}")"
    fi

    if [[ -n "${spark_selector}" ]]; then
      while read -r pod role; do
        [[ -z "${pod}" || -z "${role}" ]] && continue

        local metrics
        metrics="$(read_pod_cgroup_metrics "${pod}" || true)"
        [[ -z "${metrics}" ]] && continue

        local allocations cpu_usage_ns memory_bytes now_ns previous previous_cpu_ns previous_now_ns cpu_alloc_m memory_alloc_mi
        allocations="$(read_pod_resource_allocations "${pod}")"
        cpu_alloc_m="$(awk '{ print $1 }' <<< "${allocations}")"
        memory_alloc_mi="$(awk '{ print $2 }' <<< "${allocations}")"
        cpu_usage_ns="$(awk '{ print $1 }' <<< "${metrics}")"
        memory_bytes="$(awk '{ print $2 }' <<< "${metrics}")"
        now_ns="$(now_ns)"
        previous="$(awk -v pod="${pod}" '$1 == pod { print $2, $3 }' "${previous_sample_file}")"

        if [[ -n "${previous}" ]]; then
          previous_cpu_ns="$(awk '{ print $1 }' <<< "${previous}")"
          previous_now_ns="$(awk '{ print $2 }' <<< "${previous}")"

          awk \
            -v timestamp="${timestamp}" \
            -v app="${spark_app_name}" \
            -v pod="${pod}" \
            -v role="${role}" \
            -v cpu_usage_ns="${cpu_usage_ns}" \
            -v previous_cpu_ns="${previous_cpu_ns}" \
            -v now_ns="${now_ns}" \
            -v previous_now_ns="${previous_now_ns}" \
            -v memory_bytes="${memory_bytes}" \
            -v cpu_alloc="${cpu_alloc_m}" \
            -v mem_alloc="${memory_alloc_mi}" '
              BEGIN {
                elapsed_ns = now_ns - previous_now_ns
                cpu_delta_ns = cpu_usage_ns - previous_cpu_ns

                if (elapsed_ns <= 0 || cpu_delta_ns < 0) {
                  exit
                }

                cpu_m = cpu_delta_ns / elapsed_ns * 1000
                memory_mi = memory_bytes / 1024 / 1024
                cpu_pct = cpu_m / cpu_alloc * 100
                memory_pct = memory_mi / mem_alloc * 100

                printf "%s,%s,%s,%s,%.3f,%.3f,%.0f,%.0f,%.2f,%.2f\n", \
                  timestamp, app, pod, role, cpu_m, memory_mi, cpu_alloc, mem_alloc, cpu_pct, memory_pct
              }
            ' >> "${csv_path}"
        fi

        awk -v pod="${pod}" '$1 != pod { print }' "${previous_sample_file}" > "${previous_sample_file}.tmp"
        mv "${previous_sample_file}.tmp" "${previous_sample_file}"
        printf '%s %s %s\n' "${pod}" "${cpu_usage_ns}" "${now_ns}" >> "${previous_sample_file}"
      done < <(list_running_spark_pods "${spark_selector}")
    fi

    sleep "${SAMPLE_INTERVAL_SECONDS}"
  done

  rm -f "${previous_sample_file}"
}

summarize_spark_app() {
  local spark_app_name="$1"
  local csv_path="$2"

  awk -F, -v app="${spark_app_name}" '
    NR == 1 {
      next
    }

    $2 == app {
      sample_key = $1
      cpu_m[sample_key] += $5
      mem_mi[sample_key] += $6
      cpu_alloc[sample_key] += $7
      mem_alloc[sample_key] += $8
    }

    END {
      for (sample_key in cpu_m) {
        if (cpu_alloc[sample_key] == 0 || mem_alloc[sample_key] == 0) {
          continue
        }

        cpu_pct = cpu_m[sample_key] / cpu_alloc[sample_key] * 100
        mem_pct = mem_mi[sample_key] / mem_alloc[sample_key] * 100

        samples++
        cpu_sum += cpu_pct
        mem_sum += mem_pct

        if (samples == 1 || cpu_pct > cpu_max) {
          cpu_max = cpu_pct
        }
        if (samples == 1 || mem_pct > mem_max) {
          mem_max = mem_pct
        }
      }

      if (samples == 0) {
        printf "%s: no live Spark pod samples were captured\n", app
        exit
      }

      printf "%s: samples=%d, CPU avg/max=%.2f%%/%.2f%%, memory avg/max=%.2f%%/%.2f%%\n", \
        app, samples, cpu_sum / samples, cpu_max, mem_sum / samples, mem_max
    }
  ' "${csv_path}"
}

run_measured_job() {
  local manifest="$1"
  local job_name="$2"
  local spark_app_name="$3"
  local stop_file
  local monitor_pid
  local wait_status=0

  stop_file="$(mktemp "${TMPDIR:-/tmp}/k8s-resource-report.XXXXXX")"
  rm -f "${stop_file}"

  log "Starting measured run for ${spark_app_name}"
  kubectl delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found
  delete_old_spark_pods "${spark_app_name}"

  monitor_spark_app "${spark_app_name}" "${RESOURCE_REPORT_CSV}" "${stop_file}" &
  monitor_pid="$!"

  kubectl apply -f "${manifest}"
  wait_for_job "${job_name}" || wait_status="$?"

  touch "${stop_file}"
  wait "${monitor_pid}" 2>/dev/null || true
  rm -f "${stop_file}"

  if [[ "${wait_status}" -ne 0 ]]; then
    log "Job ${job_name} did not complete successfully"
    kubectl logs -n "${NAMESPACE}" "job/${job_name}" || true
    exit "${wait_status}"
  fi

  local driver_phase
  driver_phase="$(latest_driver_phase "${spark_app_name}")"
  if [[ "${driver_phase}" != "Succeeded" ]]; then
    log "Spark driver for ${spark_app_name} finished with phase '${driver_phase:-unknown}'"
    exit 1
  fi

  summarize_spark_app "${spark_app_name}" "${RESOURCE_REPORT_CSV}"
}

printf 'timestamp,app,pod,role,cpu_millicores,memory_mib,cpu_alloc_millicores,memory_alloc_mib,cpu_pct,memory_pct\n' > "${RESOURCE_REPORT_CSV}"

kubectl apply -f k8s/00-namespace-rbac.yaml
kubectl apply -f k8s/01-mysql.yaml
kubectl rollout status statefulset/mysql -n "${NAMESPACE}" --timeout=10m
kubectl wait --for=condition=ready pod/mysql-0 -n "${NAMESPACE}" --timeout=10m

check_metrics_api

log "Loading source table before measured jobs"
restart_job_without_measurement k8s/06-lab7-source-loader-job.yaml source-loader-submit

run_measured_job k8s/07-lab7-datamart-job.yaml datamart-submit lab8-datamart
run_measured_job k8s/08-lab7-model-job.yaml model-submit lab8-model

log "Raw samples: ${RESOURCE_REPORT_CSV}"
log "Percentages are calculated from cgroup samples against Kubernetes resource requests of each Spark driver/executor pod."
