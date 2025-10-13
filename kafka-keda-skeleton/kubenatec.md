


kubectl get pods -n {namespace}
- namespace list
  - kafka-nm : 카프라 모듈
  - order-service : 서비스 모듈
  - argocd : argocd 모듈
  - keda : keda core 모듈 

##### 서비스 시작  
kubectl apply -f ./k8s/kafka-deployment.yaml
kubectl apply -f ./k8s/keda-scaledobject.yaml

##### 서비스 삭제 및 종료 
kubectl delete -f ./k8s/kafka-deployment.yaml
kubectl delete -f ./k8s/keda-scaledobject.yaml

```bash
kafka 같이 local에 로그 및 데이터를 유지하기 위해서는 type을 stateful을 사용하게 되는데 
해당 상황에서는 서비스를 삭재해도 type이 statefulest, pvc 인경우엔 삭제되지 않고 유지된다.

때문에 기존 설정정보(host 등등)을 가지고 있을수 있음으로, statefulset 및 pvc 또한 삭제 하자
이후 다시 kubectl apply -f ... 을 실행하면 정사적인것을 확인 할 수 있다.   

# statefulset 및 pvc 삭제
kubectl delete statefulset -n kafka-nm kafka
kubectl delete pvc -n kafka-nm kafka-data-kafka-0 
```

kubectl get svc(serivce) -n kafka-nm
kubectl get pods -n kafka-nm

##### 실시간 로그 
kubectl logs -f  kafka-0 -n kafka-nm

##### 끝에서 30라인 확인
kubectl logs  kafka-0 --tail=30 -n kafka-nm

##### 상세 정보 체크 | 그중에 Events 항목만  
kubectl describe pod kafka-0 -n kafka-nm | grep -A 15 "Events:"
kubectl logs -n kafka-nm kafka-0 --tail=30 | grep -E "(Started|ERROR|UnknownHost)" | tail -10

##### order-service namespace pod 확인 
kubectl get pods -n order-service
kubectl describe pod {pod-Name}  -n order-service

##### -l 옵션을 사용하여 (레이블 셀렉터) 로그 검색
- 그냥 검색하면 pod 이름이 랜덤하게 생김으로 확인할때마다 체크해줘야 함으로 yaml 라벨의 app을 사용한다.
kubectl logs -f -l app=kafka-keda-skeleton -n order-service







