# ray-operator

ray-operator用于协调管理ray集群的创建、销毁、升级、扩缩容等操作，以及ray任务的管理等功能。

ray-operator/main.go中启动时会进行一些初始化操作，包括设置监听namespace、日志编码器、批处理调度器等，之后创建controllermanager和raycluster、rayjob、rayservice等类型的controller。这些controller会监听对应的资源，并调用对应的reconciler进行资源处理。

```go
	exitOnError(ray.NewReconciler(ctx, mgr, rayClusterOptions).SetupWithManager(mgr, config.ReconcileConcurrency),
		"unable to create controller", "controller", "RayCluster")

	// Notes: 创建RayService控制器
	exitOnError(ray.NewRayServiceReconciler(ctx, mgr, config).SetupWithManager(mgr, config.ReconcileConcurrency),
		"unable to create controller", "controller", "RayService")

	exitOnError(ray.NewRayJobReconciler(ctx, mgr, rayJobOptions, config).SetupWithManager(mgr, config.ReconcileConcurrency),
		"unable to create controller", "controller", "RayJob")
```

## raycluster

RayCluster 控制器负责创建和管理 Ray 集群的生命周期、 Head 和 Worker Pods、Services 等Kubernetes 资源、处理集群的扩缩容和故障恢复。

当用户修改spec或者autoscaler触发扩缩容时，使 RayCluster 资源的 .metadata.generation 字段加一。Operator 的 Manager通过其内置的 Informer 机制监听到更新后，会将当前请求加入工作队列，等待协调。

rayClusterReconcile方法中包含多个reconcile函数，分别处理不同资源的创建、更新和删除等操作。

```go
func (r *RayClusterReconciler) rayClusterReconcile(ctx context.Context, instance *rayv1.RayCluster) (ctrl.Result, error) {
	/// ......校验

	reconcileFuncs := []reconcileFunc{
		r.reconcileAutoscalerServiceAccount,
		r.reconcileAutoscalerRole,
		r.reconcileAutoscalerRoleBinding,
		r.reconcileIngress,
		r.reconcileHeadService,
		r.reconcileHeadlessService,
		r.reconcileServeService,
		r.reconcilePods,
	}

	for _, fn := range reconcileFuncs {
		if reconcileErr = fn(ctx, instance); reconcileErr != nil {
			funcName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
			logger.Error(reconcileErr, "Error reconcile resources", "function name", funcName)
			break
		}
	}
	/// ......
}
```

以reconcilePods为例，对pods对协调主要包括对headpod的协调和对workerpod的协调两个部分。

headpod的协调包括：没有headpod时创建headpod，重启异常的headpod等。

workerpod的协调是以WorkerGroup为基础进行。WorkerGroup允许不同的工作组节点有不同的资源配置和副本数量等。

```yaml
# RayCluster配置示例
spec:
  workerGroupSpecs:
  - groupName: small-group
    replicas: 1
    minReplicas: 1
    maxReplicas: 5
    rayStartParams:
        ...
    template: # Pod template
      spec:
        ...
  # Another workerGroup
  - groupName: medium-group
    ...
  # Yet another workerGroup, with access to special hardware perhaps.
  - groupName: gpu-group
    ...
```

对于每一个workerGroup，首先删除异常和autoscale设置的待删除的workerpod，然后创建一定数量的workerpod，满足workerpod的期望数量。

```go
	for _, worker := range instance.Spec.WorkerGroupSpecs {
		// 获取workerGroup的期望副本数
		numExpectedWorkerPods := int(utils.GetWorkerGroupDesiredReplicas(ctx, worker))

		// list所有pods
		workerPods := corev1.PodList{}
		if err := r.List(ctx, &workerPods, common.RayClusterGroupPodsAssociationOptions(instance, worker.GroupName).ToListOptions()...); err != nil {
			return err
		}

		// 如果worker group被暂停，则删除所有worker pod
		if worker.Suspend != nil && *worker.Suspend {
			if _, err := r.deleteAllPods(ctx, common.RayClusterGroupPodsAssociationOptions(instance, worker.GroupName)); err != nil {
			}
		}

		// 删除所有异常的worker pod
		deletedWorkers := make(map[string]struct{})
		for _, workerPod := range workerPods.Items {
			shouldDelete, reason := shouldDeletePod(workerPod, rayv1.WorkerNode)
			if shouldDelete {
				numDeletedUnhealthyWorkerPods++
				deletedWorkers[workerPod.Name] = deleted
				if err := r.Delete(ctx, &workerPod); err != nil {
				}
			}
		}

		// 删除autoscaler设置的待删除workerpod
		for _, podsToDelete := range worker.ScaleStrategy.WorkersToDelete {
			pod := corev1.Pod{}
			pod.Name = podsToDelete
			if err := r.Delete(ctx, &pod); err != nil {
			}
		}
		worker.ScaleStrategy.WorkersToDelete = []string{}

		// 当前运行中的workerpods
		runningPods := corev1.PodList{}
		for _, pod := range workerPods.Items {
			if _, ok := deletedWorkers[pod.Name]; !ok {
				runningPods.Items = append(runningPods.Items, pod)
			}
		}

		diff := numExpectedWorkerPods - len(runningPods.Items)

		// 添加预期数量的worker pod
		if diff > 0 {
			// create all workers of this group
			for i := 0; i < diff; i++ {
				if err := r.createWorkerPod(ctx, *instance, *worker.DeepCopy()); err != nil {
				}
			}
		}  
	}
```

## autoscaler

autoscaler会不断根据集群的负载情况，对集群中pod进行扩缩容。

GCS中autoscaler的monitor进程会不断调用StandardAutoscaler的update方法，对集群进行扩缩容。

autoscaler主要包括两个部分，缩容和扩容，分别在terminate_nodes_to_enforce_config_constraints和resource_demand_scheduler.get_nodes_to_launch中实现。

terminate_nodes_to_enforce_config_constraints会终止超过 max_workers 数量的节点、终止空闲时间超过 idle_timeout_minutes 的节点和终止配置过时的节点。进行终止会使用_sort_based_on_last_used，优先保留最近使用过的节点。

get_nodes_to_launch会根据资源需求计算需要添加的节点类型和数量。

```python
    def get_nodes_to_launch(...) -> (Dict[NodeType, int], List[ResourceDict]):
        """根据资源需求计算需要添加的节点类型和数量。
        
        这是资源需求调度器的核心方法，负责计算为了满足资源需求而需要添加的节点。
        整个过程分为多个步骤，确保满足各种约束条件。

        整体流程：
            (1) 计算集群中当前的资源情况：
                - 计算每个现有节点的可用资源
                - 统计每种节点类型的节点数量
                - 包括正在运行和正在启动的节点
            (2) 计算需要添加的节点以满足每种节点类型的min_workers约束
            (3) 为每个严格分散的placement group预留空间并在必要时启动新节点
            (4) 计算未满足的资源包
            (5) 计算需要启动哪些节点来满足所有资源请求，同时遵守max_worker约束

        Returns:
            Dict[NodeType, int]: 每种类型需要添加的节点数量
            List[ResourceDict]: 仍无法满足的资源列表
        """
        # 创建部分应用的评分函数，将节点可用性摘要固定
        utilization_scorer = partial(
            self.utilization_scorer, node_availability_summary=node_availability_summary
        )
        
        # 步骤1: 计算当前集群资源和节点类型计数
        node_resources, node_type_counts = self.calculate_node_resources(
            nodes, launching_nodes, unused_resources_by_ip
        )

        # 步骤2: 添加节点以满足每种类型的min_workers
        (
            node_resources,
            node_type_counts,
            adjusted_min_workers,
        ) = _add_min_workers_nodes(...)

        # 设置placement group资源限制

        # 步骤4/5: 为待处理的任务、actors和非严格分散的placement group添加节点

        # 使用装箱算法先匹配资源需求，并返回不能满足的资源需要
        unfulfilled, _ = get_bin_pack_residual(node_resources, resource_demands)

        nodes_to_add_based_on_demand, final_unfulfilled = get_nodes_for(
            self.node_types,
            node_type_counts,
            self.head_node_type,
            max_to_add,
            unfulfilled,
            utilization_scorer=utilization_scorer,
        )
        
        # ......

        return total_nodes_to_add, final_unfulfilled
```

```python
def get_bin_pack_residual(
    node_resources: List[ResourceDict],
    resource_demands: List[ResourceDict],
    strict_spread: bool = False,
) -> (List[ResourceDict], List[ResourceDict]):
    """使用装箱算法计算无法满足的资源需求。
    
    该函数实现了资源分配的装箱算法，尝试将资源需求分配到节点上，
    返回无法满足的资源需求和节点上剩余的资源。

    Args:
        node_resources: 节点资源列表，每个元素是一个节点的资源字典
        resource_demands: 资源需求列表，每个元素是一个资源需求字典
        strict_spread: 是否严格分散模式。如果为True，每个资源需求必须放在不同的节点上

    Returns:
        List[ResourceDict]: 无法满足的资源需求列表
        List[ResourceDict]: 分配后的节点资源列表（更新后的资源）
    """

    # 记录无法满足的资源需求
    unfulfilled = []

    # 创建节点资源的深拷贝，避免修改原始数据
    nodes = copy.deepcopy(node_resources)
    
    # 在严格分散模式下，记录已使用的节点索引
    used = set()
    
    # 按复杂度对资源需求进行排序，确保复杂的需求优先处理
    # 排序规则：1. 资源类型数量多的优先 2. 资源总量大的优先 3. 字典序
    for demand in sorted(
        resource_demands,
        key=lambda demand: (
            len(demand.values()),
            sum(demand.values()),
            sorted(demand.items()),
        ),
        reverse=True,
    ):
        # 标记是否找到合适的节点
        found = False
        node = None
        
        # 遍历所有节点，寻找能满足该资源需求的节点
        for i in range(len(nodes)):
            # 在严格分散模式下，跳过已使用的节点
            if i in used:
                continue
                
            node = nodes[i]
            
            # 检查节点是否能满足该资源需求
            if _fits(node, demand):
                found = True
                
                # 在严格分散模式下，标记该节点已被使用
                if strict_spread:
                    used.add(i)
                break
                
        # 如果找到合适的节点，则从节点资源中扣除该需求
        if found and node:
            _inplace_subtract(node, demand)
        else:
            # 如果没有找到合适的节点，则将该需求加入未满足列表
            unfulfilled.append(demand)

    return unfulfilled, nodes
```

对于没有满足资源要求的资源，get_nodes_for会循环处理资源需求，通过对计算每种节点类型的适用性得分，根据得分排序，选择得分最高的节点类型、增加选中节点类型的计数，然后利用装箱算法get_bin_pack_residual，从资源需求中移除已满足的部分，然后不断循环直到满足需求或者所有资源都达到最大限制。

评分函数是autoscaler核心，评分策略包括：
1、避免不必要的 GPU 节点：如果启用了 AUTOSCALER_CONSERVE_GPU_NODES 且任务不需要 GPU，则避免选择 GPU 节点
2、匹配资源类型数量：优先选择能匹配更多资源类型需求的节点
3、资源利用率：优先选择资源利用率更高的节点
4、资源平衡：考虑多个资源类型的平衡利用情况

```python
def _resource_based_utilization_scorer(
    node_resources: ResourceDict,
    resources: List[ResourceDict],
    *,
    node_availability_summary: NodeAvailabilitySummary,
) -> Optional[Tuple[bool, int, float, float]]:
    """基于资源的节点适用性评分函数。
    
    该函数计算给定节点类型对一组资源需求的适用性评分。
    评分考虑了多个因素，包括GPU使用策略、资源类型匹配度、资源利用率等。

    Args:
        node_resources: 节点类型的资源字典，如 {"CPU": 4, "GPU": 1, "memory": 1024}
        resources: 待满足的资源需求列表
        node_availability_summary: 节点可用性摘要信息

    Returns:
        Optional[Tuple[bool, int, float, float]]: 一个四元组评分，包含：
            - bool: 是否可以使用GPU节点
            - int: 匹配的资源类型数量
            - float: 最小资源利用率
            - float: 平均资源利用率
        如果节点无法满足任何资源需求，则返回None
    """
    # 深拷贝节点资源，避免修改原始数据
    remaining = copy.deepcopy(node_resources)
    
    # 记录可以满足的资源需求
    fittable = []
    
    # 记录所有需要的资源类型
    resource_types = set()
    
    # 遍历所有资源需求
    for r in resources:
        # 收集该资源需求中所有非零的资源类型
        for k, v in r.items():
            if v > 0:
                resource_types.add(k)
                
        # 检查节点是否能满足该资源需求
        if _fits(remaining, r):
            # 如果能满足，则记录该需求，并从剩余资源中扣除
            fittable.append(r)
            _inplace_subtract(remaining, r)
            
    # 如果没有任何资源需求可以满足，则返回None
    if not fittable:
        return None

    # 计算各资源类型的利用率
    util_by_resources = []
    
    # 记录匹配的资源类型数量
    num_matching_resource_types = 0
    
    # 遍历节点的所有资源类型
    for k, v in node_resources.items():
        # 跳过值小于1的资源（避免除零错误）
        if v < 1:
            continue
            
        # 如果该资源类型在需求中出现过，则增加匹配计数
        if k in resource_types:
            num_matching_resource_types += 1
            
        # 计算该资源类型的利用率（使用立方加权，更强调高利用率）
        util = (v - remaining[k]) / v
        util_by_resources.append(v * (util**3))

    # 如果没有可用的资源利用率数据，则返回None
    if not util_by_resources:
        return None

    # 根据AUTOSCALER_CONSERVE_GPU_NODES设置决定是否可以使用GPU节点
    gpu_ok = True
    
    # 如果启用了GPU节点保护模式
    if AUTOSCALER_CONSERVE_GPU_NODES:
        # 检查节点是否为GPU节点
        is_gpu_node = "GPU" in node_resources and node_resources["GPU"] > 0
        
        # 检查资源需求中是否包含GPU需求
        any_gpu_task = any("GPU" in r for r in resources)
        
        # 如果是GPU节点但资源需求不包含GPU，则不使用该节点
        if is_gpu_node and not any_gpu_task:
            gpu_ok = False

    # 返回四元组评分，按优先级排序：
    # 1. 是否可以使用GPU节点（避免不必要的GPU节点）
    # 2. 匹配的资源类型数量（匹配度越高越好）
    # 3. 最小资源利用率（避免资源浪费）
    # 4. 平均资源利用率（整体利用率越高越好）
    return (
        gpu_ok,
        num_matching_resource_types,
        min(util_by_resources),
        # util_by_resources 应该是非空的
        float(sum(util_by_resources)) / len(util_by_resources),
    )
```

autoscaler最终调用provider去和kuberay交互，通过_patch()方法向Kubernetes API发送PATCH请求，修改RayCluster CR的replicas字段。