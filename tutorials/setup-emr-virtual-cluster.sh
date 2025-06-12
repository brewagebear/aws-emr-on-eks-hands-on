#!/bin/bash

# 간단한 EMR Virtual Cluster 설정 스크립트 (MWAA 없는 버전)
PROJECT_NAME="spark-on-eks-hands-on"
REGION="ap-northeast-2"
CLUSTER_NAME="${PROJECT_NAME}-cluster"

echo "🚀 EMR Virtual Cluster 및 Spark Operator 설정 (간단한 버전)"
echo "============================================================"

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

# 에러 핸들링
set -e
trap 'log_error "설정 중 오류 발생. 라인 $LINENO에서 중단됨"' ERR

# 사전 검사
check_prerequisites() {
    log_info "사전 요구사항 확인..."

    # 필수 도구 확인
    local missing_tools=()

    for tool in aws kubectl helm; do
        if ! command -v $tool >/dev/null 2>&1; then
            missing_tools+=($tool)
        fi
    done

    if [ ${#missing_tools[@]} -ne 0 ]; then
        log_error "다음 도구들이 설치되지 않았습니다: ${missing_tools[*]}"
        echo ""
        echo "설치 방법:"
        echo "AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        echo "kubectl: curl -LO \"https://dl.k8s.io/release/\$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl\""
        echo "helm: curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
        exit 1
    fi

    # AWS 자격 증명 확인
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS 자격 증명이 설정되지 않았습니다."
        echo "다음 명령을 실행하세요: aws configure"
        exit 1
    fi

    # CloudFormation 스택 확인
    if ! aws cloudformation describe-stacks --stack-name $PROJECT_NAME --region $REGION &> /dev/null; then
        log_error "CloudFormation 스택이 존재하지 않습니다."
        echo "먼저 다음을 실행하세요: ./simple_deploy.sh"
        exit 1
    fi

    log_success "사전 요구사항 확인 완료"
}

# EKS 클러스터 연결
setup_eks_connection() {
    log_info "EKS 클러스터 연결 설정..."

    # kubeconfig 업데이트
    aws eks update-kubeconfig --region $REGION --name $CLUSTER_NAME

    # 클러스터 접근 확인
    if kubectl get nodes >/dev/null 2>&1; then
        local node_count=$(kubectl get nodes --no-headers | wc -l)
        log_success "EKS 클러스터 연결 성공 (노드 수: $node_count)"

        echo "현재 노드 상태:"
        kubectl get nodes
    else
        log_error "EKS 클러스터에 접근할 수 없습니다."
        echo "클러스터가 완전히 생성될 때까지 기다린 후 다시 시도하세요."
        echo "클러스터 상태 확인: aws eks describe-cluster --name $CLUSTER_NAME --region $REGION"
        exit 1
    fi
}

# Kubernetes 네임스페이스 및 RBAC 설정
setup_kubernetes_resources() {
    log_info "Kubernetes 리소스 설정..."

    # 네임스페이스 생성
    kubectl create namespace spark-operator --dry-run=true -o yaml | kubectl apply -f -
    kubectl create namespace emr --dry-run=true -o yaml | kubectl apply -f -

    log_success "네임스페이스 생성 완료 (spark-operator, emr)"

    # EMR 실행 역할 ARN 가져오기
    local EMR_ROLE_ARN=$(aws cloudformation describe-stacks \
        --stack-name $PROJECT_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`EMRExecutionRoleArn`].OutputValue' \
        --output text \
        --region $REGION)

    log_info "EMR 서비스 계정 및 RBAC 설정 중..."

    # EMR을 위한 서비스 계정 및 RBAC 설정
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: emr-containers-sa-spark
  namespace: emr
  annotations:
    eks.amazonaws.com/role-arn: $EMR_ROLE_ARN
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: emr
  name: emr-containers-role-spark
rules:
- apiGroups: [""]
  resources: ["pods", "services", "endpoints", "persistentvolumeclaims", "events", "configmaps", "secrets"]
  verbs: ["*"]
- apiGroups: [""]
  resources: ["namespaces"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["apps"]
  resources: ["deployments", "daemonsets", "replicasets", "statefulsets"]
  verbs: ["*"]
- apiGroups: ["extensions"]
  resources: ["ingresses"]
  verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: emr-containers-rb-spark
  namespace: emr
subjects:
- kind: ServiceAccount
  name: emr-containers-sa-spark
  namespace: emr
roleRef:
  kind: Role
  name: emr-containers-role-spark
  apiGroup: rbac.authorization.k8s.io
EOF

    log_success "EMR RBAC 설정 완료"
}

# Spark Operator 설치
install_spark_operator() {
    log_info "Spark Operator 설치..."

    # Helm 리포지토리 추가
    helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator

    # 기존 설치 확인
    if helm list  -n spark-operator | grep -q spark-operator; then
        log_warning "Spark Operator가 이미 설치되어 있습니다. 업그레이드 중..."
        helm upgrade spark-operator spark-operator/spark-operator \
            --namespace spark-operator \
            --set webhook.enable=true \
            --set metrics.enable=true \
            --set metrics.port=10254 \
            --set sparkJobNamespace=emr
    else
        helm install spark-operator spark-operator/spark-operator \
            --namespace spark-operator \
            --set webhook.enable=true \
            --set metrics.enable=true \
            --set metrics.port=10254 \
            --set sparkJobNamespace=emr
    fi

    # Spark Operator 상태 확인
    log_info "Spark Operator 준비 대기 중..."
    kubectl wait --for=condition=available deployment \
    -l app.kubernetes.io/name=spark-operator \
    --timeout=300s -n spark-operator

    log_success "Spark Operator 설치 완료"
}

# EMR Virtual Cluster 생성
create_emr_virtual_cluster() {
    log_info "EMR Virtual Cluster 생성..."

    # 기존 Virtual Cluster 확인
    local existing_cluster=$(aws emr-containers list-virtual-clusters \
        --query "virtualClusters[?name=='${PROJECT_NAME}-virtual-cluster' && state=='RUNNING'].id" \
        --output text \
        --region $REGION)

    if [ ! -z "$existing_cluster" ] && [ "$existing_cluster" != "None" ]; then
        log_warning "Virtual Cluster가 이미 존재합니다: $existing_cluster"
        VIRTUAL_CLUSTER_ID=$existing_cluster
    else
        # 새 Virtual Cluster 생성
        # EKS API and ConfigMap 설정을 해야합니다.
        log_info "새로운 Virtual Cluster 생성 중..."
        VIRTUAL_CLUSTER_ID=$(aws emr-containers create-virtual-cluster \
            --name "${PROJECT_NAME}-virtual-cluster" \
            --container-provider '{
                "type": "EKS",
                "id": "'$CLUSTER_NAME'",
                "info": {
                    "eksInfo": {
                        "namespace": "emr"
                    }
                }
            }' \
            --region $REGION \
            --query 'id' \
            --output text)

        # Virtual Cluster 상태 확인
        log_info "Virtual Cluster 생성 대기 중..."
        local max_attempts=20
        local attempt=0

        while [ $attempt -lt $max_attempts ]; do
            local cluster_state=$(aws emr-containers describe-virtual-cluster \
                --id $VIRTUAL_CLUSTER_ID \
                --query 'virtualCluster.state' \
                --output text \
                --region $REGION)

            case $cluster_state in
                "RUNNING")
                    log_success "Virtual Cluster 준비 완료"
                    break
                    ;;
                "ARRESTED"|"TERMINATED")
                    log_error "Virtual Cluster 생성 실패: $cluster_state"
                    exit 1
                    ;;
                *)
                    echo "   ⏳ Virtual Cluster 상태: $cluster_state (시도 $((attempt+1))/$max_attempts)"
                    sleep 15
                    attempt=$((attempt+1))
                    ;;
            esac
        done

        if [ $attempt -eq $max_attempts ]; then
            log_error "Virtual Cluster 생성 타임아웃"
            exit 1
        fi
    fi

    log_success "Virtual Cluster ID: $VIRTUAL_CLUSTER_ID"
}

# 테스트 Spark 애플리케이션 생성
create_test_spark_app() {
    log_info "테스트 Spark 애플리케이션 매니페스트 생성..."

    cat > test-spark-pi.yaml << EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-pi-test
  namespace: emr
spec:
  type: Scala
  mode: cluster
  image: public.ecr.aws/emr-on-eks/spark/emr-6.15.0:latest
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///usr/lib/spark/examples/jars/spark-examples.jar
  arguments:
    - "100"
  sparkVersion: "3.4.1"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    coreLimit: "1000m"
    memory: "1g"
    serviceAccount: emr-containers-sa-spark
    labels:
      version: 3.4.1
    env:
      - name: AWS_REGION
        value: "$REGION"
  executor:
    cores: 1
    instances: 2
    memory: "1g"
    labels:
      version: 3.4.1
    env:
      - name: AWS_REGION
        value: "$REGION"
EOF

    log_success "테스트 Spark 애플리케이션 매니페스트 생성 완료"
    echo "   파일: test-spark-pi.yaml"
    echo "   테스트 실행: kubectl apply -f test-spark-pi.yaml"
    echo "   상태 확인: kubectl get sparkapplications -n emr"
    echo "   로그 확인: kubectl logs -f [pod-name] -n emr"
}

# 환경 정보 업데이트
update_environment_info() {
    log_info "환경 정보 업데이트..."

    # .env 파일 업데이트
    if [ -f ".env" ]; then
        # Virtual Cluster ID 추가
        if grep -q "VIRTUAL_CLUSTER_ID" .env; then
            sed -i "s/VIRTUAL_CLUSTER_ID=.*/VIRTUAL_CLUSTER_ID=$VIRTUAL_CLUSTER_ID/" .env
        else
            echo "VIRTUAL_CLUSTER_ID=$VIRTUAL_CLUSTER_ID" >> .env
        fi
    fi

    # 업데이트된 작업 제출 스크립트 생성
    cat > submit_test_spark_job.py << EOF
#!/usr/bin/env python3
"""
간단한 테스트 Spark 작업 제출 스크립트
"""
import boto3
import time
from datetime import datetime

VIRTUAL_CLUSTER_ID = "$VIRTUAL_CLUSTER_ID"
REGION = "$REGION"
PROJECT_NAME = "$PROJECT_NAME"

def submit_pi_calculation():
    """간단한 Pi 계산 작업 제출"""
    emr_client = boto3.client('emr-containers', region_name=REGION)

    # EMR 역할 ARN 가져오기
    cf_client = boto3.client('cloudformation', region_name=REGION)
    response = cf_client.describe_stacks(StackName=PROJECT_NAME)

    emr_role_arn = None
    for output in response['Stacks'][0]['Outputs']:
        if output['OutputKey'] == 'EMRExecutionRoleArn':
            emr_role_arn = output['OutputValue']
            break

    if not emr_role_arn:
        print("❌ EMR 실행 역할을 찾을 수 없습니다.")
        return None

    job_name = f"pi-calculation-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    job_config = {
        "name": job_name,
        "virtualClusterId": VIRTUAL_CLUSTER_ID,
        "executionRoleArn": emr_role_arn,
        "releaseLabel": "emr-6.15.0-latest",
        "jobDriver": {
            "sparkSubmitJobDriver": {
                "entryPoint": "local:///usr/lib/spark/examples/jars/spark-examples.jar",
                "entryPointArguments": ["100"],
                "sparkSubmitParameters": (
                    "--class org.apache.spark.examples.SparkPi "
                    "--conf spark.executor.instances=2 "
                    "--conf spark.executor.memory=1g "
                    "--conf spark.executor.cores=1 "
                    "--conf spark.driver.memory=1g"
                )
            }
        },
        "configurationOverrides": {
            "monitoringConfiguration": {
                "cloudWatchMonitoringConfiguration": {
                    "logGroupName": f"/aws/emr-containers/{PROJECT_NAME}",
                    "logStreamNamePrefix": "pi-test"
                }
            }
        }
    }

    print(f"🚀 Pi 계산 작업 제출 중: {job_name}")

    try:
        response = emr_client.start_job_run(**job_config)
        job_id = response['id']

        print(f"✅ 작업 제출 성공!")
        print(f"   작업 ID: {job_id}")
        print(f"   Virtual Cluster: {VIRTUAL_CLUSTER_ID}")

        return job_id

    except Exception as e:
        print(f"❌ 작업 제출 실패: {e}")
        return None

def check_job_status(job_id):
    """작업 상태 확인"""
    emr_client = boto3.client('emr-containers', region_name=REGION)

    try:
        response = emr_client.describe_job_run(
            virtualClusterId=VIRTUAL_CLUSTER_ID,
            id=job_id
        )

        job_run = response['jobRun']
        print(f"📊 작업 상태: {job_run['state']}")

        if 'stateDetails' in job_run:
            print(f"   세부사항: {job_run['stateDetails']}")

        return job_run['state']

    except Exception as e:
        print(f"❌ 상태 확인 실패: {e}")
        return None

if __name__ == "__main__":
    print("🧮 EMR on EKS Pi 계산 테스트")
    print("=" * 40)

    job_id = submit_pi_calculation()

    if job_id:
        print("\\n작업 상태 확인:")
        print(f"aws emr-containers describe-job-run --virtual-cluster-id {VIRTUAL_CLUSTER_ID} --id {job_id}")

        # 간단한 상태 확인
        time.sleep(5)
        check_job_status(job_id)
EOF

    chmod +x submit_test_spark_job.py
    log_success "환경 정보 업데이트 완료"
}

# 상태 확인 및 검증
verify_setup() {
    log_info "설정 검증 중..."

    echo "📊 현재 상태:"
    echo ""

    # EKS 노드 상태
    echo "🚢 EKS 노드:"
    kubectl get nodes
    echo ""

    # Spark Operator 상태
    echo "⚡ Spark Operator:"
    kubectl get pods -n spark-operator
    echo ""

    # EMR 네임스페이스 리소스
    echo "📦 EMR 네임스페이스:"
    kubectl get all -n emr
    echo ""

    # Virtual Cluster 상태
    echo "🔗 Virtual Cluster:"
    aws emr-containers describe-virtual-cluster \
        --id $VIRTUAL_CLUSTER_ID \
        --query 'virtualCluster.{Name:name,State:state,Id:id}' \
        --output table \
        --region $REGION

    log_success "설정 검증 완료"
}

# 완료 메시지
show_completion_message() {
    log_success "🎉 EMR Virtual Cluster 설정 완료!"
    echo "================================================"
    echo ""
    echo "📋 생성된 리소스:"
    echo "   ✅ EKS 클러스터: $CLUSTER_NAME"
    echo "   ✅ Virtual Cluster: $VIRTUAL_CLUSTER_ID"
    echo "   ✅ Spark Operator (네임스페이스: spark-operator)"
    echo "   ✅ EMR 네임스페이스 및 RBAC"
    echo ""
    echo "🚀 다음 단계:"
    echo "1. 간단한 테스트 작업 실행:"
    echo "   python3 submit_test_spark_job.py"
    echo ""
    echo "2. Kubernetes Spark 애플리케이션 테스트:"
    echo "   kubectl apply -f test-spark-pi.yaml"
    echo "   kubectl get sparkapplications -n emr"
    echo ""
    echo "3. 실제 데이터 처리 작업:"
    echo "   python3 generate_test_logs.py 10000 \$(grep DATA_BUCKET .env | cut -d'=' -f2)"
    echo "   python3 submit_spark_job.py"
    echo ""
    echo "📊 모니터링 명령어:"
    echo "   # Pod 상태 실시간 모니터링"
    echo "   kubectl get pods -n emr -w"
    echo ""
    echo "   # Spark 애플리케이션 상태"
    echo "   kubectl get sparkapplications -n emr"
    echo ""
    echo "   # 작업 로그 확인"
    echo "   kubectl logs -f [pod-name] -n emr"
    echo ""
    echo "   # EMR 작업 상태 (AWS CLI)"
    echo "   aws emr-containers list-job-runs --virtual-cluster-id $VIRTUAL_CLUSTER_ID"
    echo ""
    echo "💡 팁: 'kubectl get pods -n emr -w' 명령으로 실시간 상태를 모니터링할 수 있습니다"
}

# 메인 실행 함수
main() {
    echo "EMR Virtual Cluster 설정을 시작합니다..."
    echo "예상 소요 시간: 5-10분"
    echo ""

    read -p "계속 진행하시겠습니까? (y/N): " confirm

    if [[ ! $confirm =~ ^[Yy]$ ]]; then
        echo "설정을 취소했습니다."
        exit 0
    fi

    # 단계별 실행
    check_prerequisites
    setup_eks_connection
    setup_kubernetes_resources
    install_spark_operator
    create_emr_virtual_cluster
    create_test_spark_app
    update_environment_info
    verify_setup
    show_completion_message
}

# 스크립트 실행
main "$@"