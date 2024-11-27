## 代码结构及主要逻辑

### 代码结构

`CSVImporter`类用来导入CSV文件，包含`main`函数。

`RoadConnection`类定义了与本路链相连接的路链信息，为五元组。

`RoadChain`类定义了每个链路的拓扑结构，包含一个九元组和若干个`RoadConnection`五元组。

`neo4j`类实现了创建启动和关闭neo4j数据库的功能。

## 当前正在进行的事情

## 遇到的困难

## 如何将北京交通数据集是存储在Neo4j中

对于北京市交通数据集（`Topo.csv`）来说，由于节点实际上是我们根据道路之间的拓扑关系自己虚构合并出来的，所以我只为这些节点分配了一个`int`类型的唯一索引。可以由map来根据链路索引（网格号+链路号）查找对应的开始节点索引和结束节点索引。

对于边来说，由于我们的任务较为简单，所以边的属性我只存了`int`类型的`gridId`和`chainId`，分别代表这条链路的网格号和链路号。

合并节点的逻辑：每条链路在`Topo.csv`文件中的保存形式是：前 9 项数据表示一个路链的基本信息，之后紧接着若干个 5 元组，每个 5 元组表示 1 个与本路链相连接的路链信息。我们先用一个map保存从链路索引（网格号+链路号的`Pair`）到开始`节点索引和结束节点索引的Pair`的映射。如果一条链路的某条入链在`map`中，我们就返回对应的`endNode`作为这条链路的`startNode`，相反我们就创建一个`startNode`；如果这条链路的某条出链在`map`中，那我们就返回对应的`startNode`作为这条链路的`endNode`，相反我们就创建一个`endNode`。再将`startNode`和`endNode`链路加入`Neo4j`数据库和`map`中。

导入北京道路交通数据集数据结构（动态路况信息）思路很简单，只需一直在创建边即可，数据集的每一行对应一条新边。