#### helm argocd 실행
```bash
helm repo add argo https://argoproj.github.io/argo-helm
helm repo update
helm install argocd argo/argo-cd -n kafka-eda


 
```

##### ArgoCD 에서 인그레스 적용을 위함
- 이미 설치된 내역이 있을경우 install 명령어가 아니라 upgrade 명령어를 사용할것 

```
# 설치 명령어  
helm install <your-release-name> argo/argo-cd --namespace <your-namespace> -f values.yaml

# 신규 설치 
helm install argocd argo/argo-cd  -n kafka-eda   -f values.yaml

# 기존설치에 업그레이드 
helm upgrade argocd argo/argo-cd  -n kafka-eda   -f values.yaml

# 삭제 
helm uninstall argocd
```


##### 헬름 기본 명령어 
######  챠트 설치 및 설정 관리
- helm install <<release-name>> <<chart-name>> --namespace <<namespace>>
- helm list : 현재 네임스페이스에 설치된 릴리즈 목록을 표시 합니다. 
- helm upgrade <<release-name>> <<chart-name>> : 이미 설치된 릴리즈를 업그레이드 합니다. 
- helm uninstall <<release-name>> : 지정된 릴리즈를 삭제 합니다.
- helm rollback <<release-name>> <<revision>> : 과거 버전의 릴리즈로 롤백한다. 

###### 챠트 검색 및 저장소 관리  
- helm search repo <<keyword>> : 등록된 챠트 검색
- helm repo add <<repo-name>> <<repo-url>> : 새로운 차트 저장소 추가
- helm repo update : 저장소 업데이트하여 최신 목록 가져오기 

###### 정보 확인 
- helm status <<release-name>> : 릴리즈 현재 상태 확인 
- helm history <<release-name>> : 릴리즈 버전 이력 확인 
- helm get values <<release-name>> : 릴리즈에 사용된 값을 보여줍니다. 

###### 패키징 및 템플릿 
- helm create <<chart-name>> : 새로운 차트 구조 생성
- helm lint <<chart-path>> : 생성된 챠트 검사하여 문제 확인


