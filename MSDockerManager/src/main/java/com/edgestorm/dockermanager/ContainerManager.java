package com.edgestorm.dockermanager;

import java.nio.file.Paths;
import java.time.Period;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.AttachParameter;
import com.spotify.docker.client.DockerClient.LogsParam;
import com.spotify.docker.client.ProgressHandler;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ContainerStats;
import com.spotify.docker.client.messages.ContainerUpdate;
import com.spotify.docker.client.messages.CpuStats.CpuUsage;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageSearchResult;
import com.spotify.docker.client.messages.Network;
import com.spotify.docker.client.messages.NetworkConfig;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.docker.client.messages.ProgressMessage;
import com.spotify.docker.client.messages.TopResults;

public class ContainerManager {
	
	/*memory limits (MB)*/
	private final static long memoryLimit = 256;
	/*CPU limits (%)*/
	private final static long cpuLimit = 50;
	
	private final static long period =100000;
	private final static long quota = cpuLimit * 1000;
	
	private final static long KB = 1024;
	private final static long MB = 1024 * KB;
	
	//An instance of docker client
	private DockerClient client;
	
	private final String dockerDirectory = "/home/cmy/workspace/tamugithub/EdgeStorm/Code/Research/MSDockerManager/src/main/resources/";
	private final String imageName = "ms_worker1";
	
	//build image
	final AtomicReference<String> imageIdFromMessage = new AtomicReference<>();
	
	public Map<String, List<PortBinding>> portBindings = new HashMap<>();
	
	public static void main(String[] args) throws Exception {
		new ContainerManager().test();
	}	

	/**
	 * Construction function
	 * @throws Exception
	 */
	public ContainerManager() throws Exception {
		init();
	}
	
	/**
	 * Initial the key variables in Docker Manager
	 */
	private void init() throws Exception {
		// Create a client based on DOCKER_HOST and DOCKER_CERT_PATH env vars
		client = DefaultDockerClient.fromEnv().build();
	}
	
	/**
	 * create Container instance based on information
	 * @param imageName: a string of image name
	 */
	private ContainerCreation createContainer(String name) throws Exception {
		
		//search image locally
		//final List<ImageSearchResult> searchResult = client.searchImages(name);
		
		//if (searchResult != null) {
		//	System.out.println("successfully locate local image " + name);
		//}
		//else {
		//	System.out.println("Fails to locate local image " + name + ". try to build from dockerfile");
	//	}
		
		//build docker image locally
		final String returnedImageId = client.build(
		    Paths.get(dockerDirectory), name, new ProgressHandler() {
		      @Override
		      public void progress(ProgressMessage message) throws DockerException {
		        final String imageId = message.buildImageId();
		        if (imageId != null) {
		          imageIdFromMessage.set(imageId);
		          System.out.println("Successfully create image " + name);
		        }
		      }
		    });
		
				// Create container
				ContainerCreation container = client.createContainer(ContainerConfig
					.builder()
					.image(name)
					.env()
					.exposedPorts("3306")
					.hostConfig(
						HostConfig
							.builder()
							.networkMode("host")
							.portBindings(
								ImmutableMap.of(
									"3306", 
									ImmutableList.of(
										PortBinding.of("0.0.0.0", 3306)
									)
								)
							)
							.build()
					)
					.build()
				);
						
				System.out.println("\n=== client.createContainer");
				System.out.println(container);
				
				
				return container;
	}
	
	/**
	 * start a container instance
	 * @param containerID
	 * @throws Exception
	 */
	public void startContainer(String containerID) throws Exception {
		this.client.startContainer(containerID);
	}
	
	/**
	 * stop a container instance
	 * @param containerID
	 * @param s: seconds before killing the container instance
	 * @throws Exception
	 */
	public void stopContainer(String containerID, int s) throws Exception {
		client.stopContainer(containerID, s /* wait s seconds before killing */);
	}
	
	
	/**
	 * pause a container instance
	 * @param containerID
	 * @throws Exception
	 */
	public void pauseContainer(String containerID) throws Exception {
		client.pauseContainer(containerID);
	}
	
	/**
	 * resume a container instance
	 * @param containerID
	 * @throws Exception
	 */
	public void resumeContainer(String containerID) throws Exception {
		client.unpauseContainer(containerID);
	}
	
	/**
	 * remove a container instance
	 * @param containerID
	 * @throws Exception
	 */
	public void removeContainer(String containerID) throws Exception{
		client.removeContainer(containerID);	
	}
	
	/**
	 * inspect a container instance
	 * @param containerID
	 * @return ContainerInfo: the information of container
	 * @throws Exception
	 */
	public ContainerInfo inspectContainer(String containerID) throws Exception{
		ContainerInfo info = client.inspectContainer(containerID);
		System.out.println("\n=== client.inspectContainer");
		System.out.println(info);
		
		return info;
	}
	

	/**
	 * Get process info in a container instance
	 * @param containerID
	 * @return TopResults: process information returned from top command
	 * @throws Exception
	 */
	public TopResults getProcessInfo(String containerID) throws Exception{
		TopResults top = client.topContainer(containerID);
		System.out.println("\n=== client.topContainer");
		System.out.println(top);
		
		return top;
	}
	
	/**
	 * update memory limits of a container instance
	 * @param memoryLimits: memory limits
	 */
	public void updateMemoryLimits(String containerID, long memoryLimits) throws Exception{
		// Update container
		final ContainerUpdate update = client.updateContainer(containerID, 
			HostConfig
				.builder()
				.memory(memoryLimits)
				.memorySwap(-1L)
				.build()
			);
		System.out.println("\n=== client.updateContainer");
		System.out.println(update);
	}
	
	/**
	 * update CPU shares of a container instance (note, CPU share is a relative value)
	 * @param containerID
	 * @param cpuShares
	 * @throws Exception
	 */
	public void updateCPUShares(String containerID, long cpuShares) throws Exception{
		// Update container
		final ContainerUpdate update = client.updateContainer(containerID, 
			HostConfig
				.builder()
				.cpuShares(cpuShares)
				.build()
			);
		System.out.println("\n=== client.updateContainer");
		System.out.println(update);
	}
	
	/**
	 * update CPU period of a container instance (more info: 
	 * https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/sec-cpu)
	 * @param containerID
	 * @param cpuPeriod
	 * @throws Exception
	 */
	public void updateCPUPeriod(String containerID, long cpuPeriod) throws Exception{
		// Update container
		final ContainerUpdate update = client.updateContainer(containerID, 
			HostConfig
				.builder()
				.cpuPeriod(cpuPeriod)
				.build()
			);
		System.out.println("\n=== client.updateContainer");
		System.out.println(update);
	}
	
	/**
	 * update CPU quota of a container instance (more info:
	 * https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/sec-cpu)
	 * @param containerID
	 * @param cpuQuota
	 * @throws Exception
	 */
	public void updateCPUQuota(String containerID, long cpuQuota) throws Exception{
		// Update container
		final ContainerUpdate update = client.updateContainer(containerID, 
			HostConfig
				.builder()
				.cpuQuota(cpuQuota)
				.build()
			);
		System.out.println("\n=== client.updateContainer");
		System.out.println(update);
	}
	
	/**
	 * get the memory usage of a container instance
	 * @param containerID
	 * @return long: usgae limits
	 * @throws Exception
	 */
	public long getMemoryUsage(String containerID) throws Exception{
		return client.stats(containerID).memoryStats().usage();
	}
	
	/**
	 * get the memory limits of a container instance
	 * @param containerID
	 * @return long: memory limits
	 * @throws Exception
	 */
	public long getMemoryLimit(String containerID) throws Exception{
		return client.stats(containerID).memoryStats().limit();
	}
	
	/**
	 * get the memory limits of a container instance
	 * @param containerID
	 * @return CpuUsage
	 * @throws Exception
	 */
	public CpuUsage getCPUUsage(String containerID) throws Exception{
		return client.stats(containerID).cpuStats().cpuUsage();
	}
	
	/**
	 * get the cumulative CPU usage percentage
	 * @param containerID
	 * @return CPU usage percentage
	 * @throws Exception
	 */
	public double getCPUPercentage(String containerID) throws Exception{
		double useageInPercent = 0.0;
		ContainerStats stats = client.stats(containerID);
		
		
		long totalCPU = stats.cpuStats().cpuUsage().totalUsage();
		long totalCPUPrev = stats.precpuStats().cpuUsage().totalUsage();
		long deltaCPU = totalCPU - totalCPUPrev;
		
		long totalSystem = stats.cpuStats().systemCpuUsage();
		long totalSystemPrev = stats.precpuStats().systemCpuUsage();
		long deltaSystem = totalSystem-totalSystemPrev;
		
		if(deltaCPU > 0 && deltaSystem > 0) {
			useageInPercent =  deltaCPU*1.0/deltaSystem * stats.cpuStats().cpuUsage().percpuUsage().size() * 100;
		}
		
		return useageInPercent;	
	}
	
	private void test() throws Exception {
		
		ContainerCreation container = createContainer(this.imageName);

		// Start the container
		startContainer(container.id());
	
		//inspect Container
		ContainerInfo info = inspectContainer(container.id());
		
		// Get port mappings
		final ImmutableMap<String, List<PortBinding>> mappings = info
			.hostConfig()
			.portBindings();
		System.out.println("\n=== port mappings");
		System.out.println(mappings);			

		// Get all exposed ports
		final ImmutableMap<String, List<PortBinding>> ports = info
			.networkSettings()
			.ports();
		System.out.println("\n=== ports");
		System.out.println(ports);
			
		// Get the container statistics
		long memoryLimit1 = getMemoryLimit(container.id());
		System.out.println("\n=== client.memory limits");
		System.out.println(memoryLimit1);
		
		updateMemoryLimits(container.id(), memoryLimit * MB);
		updateCPUPeriod(container.id(), period);
		updateCPUQuota(container.id(), quota);
		
		// Get the container statistics
		long memoryLimit2 = getMemoryLimit(container.id());
		System.out.println("\n=== client.memory limits");
		System.out.println(memoryLimit2);	

		client.logs(container.id(), LogsParam.stdout(), LogsParam.stderr(), LogsParam.tail(10))
		.attach(System.out, System.err, false);
		
		while(true) {
			// Get the container logs
			System.out.println("CPU usage:"+String.format("%.2f", this.getCPUPercentage(container.id()))+"%");
			System.out.println("Memory usage:"+this.getMemoryUsage(container.id())*1.0/MB +"MB");
			System.out.println();
			Thread.sleep(1000);
		}
		
//		// Stop the container for 5 seconds
//		stopContainer(container.id(), 5);
//
//		// Remove container
//		removeContainer(container.id());
//		
//		client.close();
	}
}
