# flink 提交流程


>流程
>
>1.CliFrontend   
>2.YarnJobClusterEntrypoint
>  > TaskExecutorRunner -> TaskManagerRunner
>














### CliFrontend   
    执行流程
 \> getConfigurationDirectoryFromEnv() -> 加载conf目录
 \> GlobalConfiguration.loadConfiguration(); -> 根据conf目录加载Flink yaml配置文件
 \> loadCustomCommandLines() -> 主要用于后面解析命令行用
        会创建一个ArrayList 一共添加三个CommandLine  按顺序添加
            GenericCLI > FlinkYarnSessionCli > DefaultCLI
        后面会根据 isActive 方法判断是否是活跃的,来进行相关的解析
  \> parseAndRun 用于解析命令行,并请求启动的操作      
            获取args[0]  -> get Action -> 进行witch判断,执行动作
            创建run 对应的默认配置选项 ->  即  -s -p -...
            找到活跃的customCommandLine -> 顺序Generic -> yarn -> default
            中间一些配置文件的封装,执行程序的封装
   \> executeProgram 执行封装好的program
   \> invokeMain(这个不是方法) 最终会执行用户的main方法      
   到这里就开始执行用户的main方法了,也就是需要执行用户的代码,重点在与 env.execute方法生成graph      

    TODO 进入StreamExecutionEnvironment.execute方法的执行 -> 
    
### StreamExecutionEnvironment.execute方法执行          
  \> 生成StreamGraph
  \> 将StreamGraph转换成JobGraph
  \> 启动appMaster
  \> appMaster执行我们传入的入口类,每个执行模式下是不同的 per-job模式启动了YarnJobClusterEntrypoint
  
  TODO 上面是将appMaster启动起来了 在appMaster中执行入口类
  YarnJobClusterEntrypoint >  -   per-job模式的入口类
  即执行该类的main方法
###  YarnJobClusterEntrypoint
    
    
    
    上面内容需要补齐
    
   启动dispatcherService,在内部启动了dispatcher >> 层层调用最终 调用dispatcherFactory.createDispatcher
   现在看dispatcher.start方法,看看dispatcher执行的时候都做了什么事情
    
   请求container启动TaskManager,
   
   
### YarnTaskExecutorRunner

runTaskManagerSecurely
TaskManagerRunner.runTaskManagerProcessSecurely
执行taskExecutor.start方法,通过rpcService.start调用,最终会执行到taskExecutor.onStart方法 



执行taskExecutor注册到RM中,并报告自己的slot,resource接收TaskExecutor的注册,
ResourceManager.sendSlotReport中将taskExecutor的slot注册到slotManager中(
**小细节 实际是注册到ResourceManager中的SlotManager<font color=red>实际存在SlotTracker中哦<\font>**)
并将slot添加到两个Map中一个是存储所有slot的Map,一个是所有Free的slot Map 
       
  
   
   
   
       

  
  





