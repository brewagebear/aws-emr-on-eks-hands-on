# EMR Spark on EKS 실습 가이드

## 📚 실습 개요

이 실습에서는 AWS의 다음 서비스들을 사용하여 현대적인 데이터 파이프라인을 구축합니다:

- **Amazon EKS**: Kubernetes 관리형 서비스
- **EMR on EKS**: EKS에서 실행되는 Apache Spark
- **Amazon S3**: 데이터 저장소
- **Spark Operator**: Kubernetes에서 Spark 작업 관리
- **Amazon Athena**: S3에 저장된 데이터를 SQL로 쿼리

### 🎯 학습 목표

1. EKS에서 EMR Spark 클러스터 구성
2. Spark를 이용한 로그 데이터 집계 및 분석
3. 클라우드 네이티브 데이터 파이프라인 이해
4. Athena를 통한 데이터 쿼리 및 분석

### 💰 예상 비용

**시간당 예상 비용 (서울 리전 기준)**
- EKS 제어 평면: $0.10
- EKS 워커 노드 (t3.medium Spot x2): ~$0.03
- NAT Gateway: $0.059
- 기타 (S3, CloudWatch): ~$0.01
- **총 시간당: ~$0.68** (약 ₩900)

⚠️ **중요**: 실습 완료 후 반드시 리소스를 정리하여 추가 과금을 방지하세요!

## 🚀 실습 시작하기

### 사전 준비사항

1. **AWS CLI 설치 및 구성**
```bash
# AWS CLI 설치 확인
aws --version

# AWS 자격 증명 구성
aws configure
```

2. **필수 도구 설치**
```bash
# kubectl 설치 (1.28.3 버전)
curl -o kubectl https://amazon-eks.s3.us-west-2.amazonaws.com/1.28.3/2023-11-14/bin/linux/amd64/kubectl
chmod +x ./kubectl
sudo mv ./kubectl /usr/local/bin

# eksctl 설치
curl --silent --location "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" | tar xz -C /tmp
sudo mv /tmp/eksctl /usr/local/bin

# Helm 설치 (3.x 이상)
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

### Step 1: 인프라 배포

1. **CloudFormation 템플릿 다운로드**
```bash
# 실습 디렉토리 생성
mkdir spark-on-eks-lab
cd spark-on-eks-lab

# CloudFormation 템플릿 저장
# (위에서 제공된 CloudFormation 템플릿을 emr-spark-eks-infrastructure.yaml로 저장)
```

2. **스택 배포 실행**
```bash
# 배포 스크립트 실행
chmod +x deploy.sh
./deploy.sh
```

⏳ **대기 시간**: 약 15-20분 (EKS 클러스터 생성 시간)

### Step 2: EKS 및 EMR Virtual Cluster 설정

```bash
# 설정 스크립트 실행
chmod +x setup-emr-virtual-cluster.sh
./setup-emr-virtual-cluster.sh
```

### Step 3: Spark 작업 제출 

```bash 
python3 submit_spark_job.py 
```

### Step 4: 결과 확인

1. **S3에서 결과 확인**
```bash
# 집계된 데이터 확인
aws s3 ls s3://[DATA_BUCKET]/output/ --recursive

# 로그 데이터 확인
aws s3 ls s3://[LOGS_BUCKET]/aggregated-logs/ --recursive
```

2. **각 분석 결과 이해**
- `hourly_traffic/`: 시간대별 트래픽 패턴
- `status_code_analysis/`: HTTP 상태 코드별 분석
- `url_analysis/`: URL 엔드포인트별 성능 분석
- `top_ips/`: 상위 트래픽 IP 주소
- `method_analysis/`: HTTP 메소드별 분석
- `error_logs/`: 에러 로그 상세 분석

### Step 5: Athena를 통한 데이터 쿼리

```sql 
CREATE EXTERNAL TABLE IF NOT EXISTS analytics_summary (
  total_requests BIGINT,
  unique_ips BIGINT,
  unique_sessions BIGINT,
  avg_response_time DOUBLE,
  total_bytes_sent BIGINT
)
STORED AS PARQUET
LOCATION 's3://spark-on-eks-hands-on-data-${AccountID}/output/analysis_yyyymmddd_hhmmss/summary/';
```

```sql
SELECT * FROM analytics_summary;
```

## 🔍 실습 내용 심화 학습

### 1. Spark 작업 분석

**로그 집계 스크립트 주요 기능:**
```python
# 시간별 트래픽 분석 예시
hourly_traffic = df.groupBy(
    window(col("timestamp"), "1 hour")
).agg(
    count("*").alias("request_count"),
    sum("bytes_sent").alias("total_bytes"),
    avg("response_time").alias("avg_response_time")
)
```

### 2. 비용 최적화 포인트

**실습에 적용된 비용 절약 방법:**
- Spot 인스턴스 사용 (최대 90% 할인)
- 최소 인스턴스 타입 (t3.medium)
- 단일 NAT Gateway
- 자동 스케일링으로 유휴 리소스 최소화
- S3 Lifecycle 정책으로 오래된 데이터 자동 삭제

## 🛠️ 문제 해결

### 일반적인 문제들

**1. EKS 클러스터 접근 불가**
```bash
# kubeconfig 업데이트
aws eks update-kubeconfig --region ap-northeast-2 --name spark-on-eks-demo-cluster

# 권한 확인
kubectl auth can-i "*" "*"
```

**2. Spark 작업 실패**
```bash
# EMR 작업 로그 확인
aws emr-containers describe-job-run --virtual-cluster-id [CLUSTER_ID] --id [JOB_ID]

# CloudWatch 로그 확인
aws logs describe-log-groups --log-group-name-prefix "/aws/emr-containers"
```

**3. S3 권한 오류**
```bash
# IAM 역할 권한 확인
aws iam get-role-policy --role-name spark-on-eks-demo-emr-execution-role --policy-name EMRExecutionPolicy
```

### 디버깅 팁

1. **CloudWatch Logs 활용**
    - EMR 컨테이너 로그: `/aws/emr-containers/spark-on-eks-demo`
    - MWAA 로그: `/aws/airflow/spark-on-eks-demo-mwaa`

2. **Kubernetes 디버깅**
```bash
# Pod 상태 확인
kubectl get pods -n emr
kubectl get pods -n spark-operator

# 상세 로그 확인
kubectl logs -n emr [POD_NAME]
```

## 🧪 추가 실험

### 실험 1: 다른 데이터 소스 사용

CSV 대신 JSON 로그 파일을 처리하도록 Spark 스크립트 수정:

```python
# JSON 로그 읽기
df = spark.read.json(input_path)
```

### 실험 2: 실시간 스트리밍 처리

Spark Structured Streaming을 사용한 실시간 로그 처리:

```python
# 스트리밍 DataFrame 생성
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .load()
```

### 실험 3: 다양한 집계 함수 추가

```python
# 고급 집계 함수
from pyspark.sql.functions import percentile_approx, collect_list

# 백분위수 계산
response_time_percentiles = df.agg(
    percentile_approx("response_time", 0.5).alias("median"),
    percentile_approx("response_time", 0.95).alias("p95"),
    percentile_approx("response_time", 0.99).alias("p99")
)
```

## 🧹 정리하기

실습 완료 후 다음 단계를 따라 리소스를 정리하세요:

### 수동 정리

1**Virtual Cluster 삭제**
```bash
aws emr-containers delete-virtual-cluster --id [CLUSTER_ID]
```

2**CloudFormation 스택 삭제**
```bash
aws cloudformation delete-stack --stack-name spark-on-eks-demo
```

3**S3 버킷 비우기** (필요시)
```bash
aws s3 rm s3://[BUCKET_NAME] --recursive
```

## 📝 실습 보고서 템플릿

실습 완료 후 다음 질문들에 답해보세요:

### 기술적 이해

1. **EMR on EKS의 장점은 무엇인가요?**
    - 기존 EMR 클러스터 대비 장점
    - Kubernetes 기반의 이점

2. **Spark 작업에서 어떤 최적화를 적용했나요?**
    - 파티션 설정
    - 메모리 구성
    - 압축 방식

3. **MWAA를 사용한 워크플로우 오케스트레이션의 이점은?**
    - 기존 cron job 대비 장점
    - 의존성 관리
    - 모니터링 및 알림

### 비즈니스 가치

1. **이 파이프라인이 실제 비즈니스에서 어떻게 활용될 수 있나요?**

2. **확장성 측면에서 고려해야 할 점들은?**

3. **보안 측면에서 개선할 점들은?**

### 비용 최적화

1. **실습에서 적용한 비용 최적화 방법들을 설명하세요.**

2. **프로덕션 환경에서 추가로 고려할 비용 최적화 방안은?**

## 📚 추가 학습 자료

- [Amazon EMR on EKS 개발자 가이드](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/)
- [Apache Spark 최적화 가이드](https://spark.apache.org/docs/latest/tuning.html)
- [Amazon MWAA 사용자 가이드](https://docs.aws.amazon.com/mwaa/latest/userguide/)
- [Kubernetes Spark Operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)

---

**실습 문의사항이나 문제가 발생하면 강사에게 문의하세요!** 🙋‍♂️


### 트러블 슈팅

#### Python 가상머신

```sh
❯ python3 -m venv path/to/venv
❯ source path/to/venv/bin/activate
❯ pip3 install -r requirements.txt
```

#### OICD 인증 관련

```sh
eksctl utils associate-iam-oidc-provider \
  --cluster <EKS_CLUSTER_NAME> \
  --region ap-northeast-2 \
  --approve
```

```sh
kubectl run irsa-test --rm -it --restart=Never -n emr \
  --image=amazonlinux:2 \
  --overrides='
{
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "name": "irsa-test"
  },
  "spec": {
    "serviceAccountName": "irsa-test-sa",
    "containers": [{
      "name": "aws-cli",
      "image": "amazonlinux:2",
      "command": ["/bin/sh", "-c"],
      "args": ["yum install -y aws-cli && aws sts get-caller-identity"]
    }],
    "restartPolicy": "Never"
  }
}'
```

위 쉘스크립트 결과가 정상적이면 OICD 인증이 제대로 동작함을 확인할 수 있음

### 리소스 부족으로 인한 스케줄링 실패 시

```
❌ 작업 실패: FAILED
   세부사항: JobRun timed out before spark driver pod started running due to lack of cluster resources. Last event from default-scheduler: FailedScheduling, message: 0/2 nodes are available: 1 Insufficient cpu, 2 Insufficient memory. preemption: 0/2 nodes are available: 2 No preemption victims found for incoming pod.. Please refer logs uploaded to S3/CloudWatch based on your monitoring configuration.
```

위와 같은 에러가 발생할 경우 K8S 리소스 부족으로 인한 스케줄링 실패이므로, eks node 수 자체를 증가시킨다.

```sh
aws eks update-nodegroup-config \
  --cluster-name <cluster-name> \
  --nodegroup-name <nodegroup-name> \
  --scaling-config minSize=2,maxSize=5,desiredSize=4 \
  --region ap-northeast-2
```