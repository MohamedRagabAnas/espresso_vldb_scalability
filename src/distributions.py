import random
import numpy as np
from collections import defaultdict

class DistributionStrategies:
    @staticmethod
    def uniform_distribution(items, count):
        """
        Distribute items uniformly
        
        Args:
            items: List of items to distribute
            count: Number of groups to distribute to
            
        Returns:
            List of counts for each group
        """
        if count <= 0:
            return []
        
        base_count = len(items) // count
        remainder = len(items) % count
        
        distribution = [base_count] * count
        for i in range(remainder):
            distribution[i] += 1
        
        return distribution
    
    @staticmethod
    def pareto_distribution(items, count, alpha=1.5, min_count=1):
        """
        Distribute items using Pareto distribution
        
        Args:
            items: List of items to distribute
            count: Number of groups to distribute to
            alpha: Shape parameter for Pareto distribution
            min_count: Minimum number of items per group
            
        Returns:
            List of counts for each group
        """
        if count <= 0:
            return []
        
        # Generate Pareto-distributed weights
        weights = np.random.pareto(alpha, count)
        weights = np.maximum(weights, 0.1)  # Ensure no zero weights
        weights = weights / weights.sum()  # Normalize to sum to 1
        
        # Calculate distribution based on weights
        distribution = (weights * len(items)).astype(int)
        
        # Ensure minimum count
        distribution = np.maximum(distribution, min_count)
        
        # Adjust to ensure we distribute exactly all items
        total_distributed = distribution.sum()
        diff = len(items) - total_distributed
        
        if diff > 0:
            # Add remaining items to groups with highest weights
            sorted_indices = np.argsort(weights)[::-1]
            for i in range(diff):
                distribution[sorted_indices[i % count]] += 1
        elif diff < 0:
            # Remove excess items from groups with lowest weights
            sorted_indices = np.argsort(weights)
            for i in range(-diff):
                idx = sorted_indices[i % count]
                if distribution[idx] > min_count:
                    distribution[idx] -= 1
        
        return distribution.tolist()
    
    @staticmethod
    def zipf_distribution(items, count, alpha=1.2, min_count=1):
        """
        Distribute items using Zipf distribution
        
        Args:
            items: List of items to distribute
            count: Number of groups to distribute to
            alpha: Exponent parameter for Zipf distribution
            min_count: Minimum number of items per group
            
        Returns:
            List of counts for each group
        """
        if count <= 0:
            return []
        
        # Generate Zipf-distributed weights
        ranks = np.arange(1, count + 1)
        weights = 1 / np.power(ranks, alpha)
        weights = weights / weights.sum()  # Normalize to sum to 1
        
        # Calculate distribution based on weights
        distribution = (weights * len(items)).astype(int)
        
        # Ensure minimum count
        distribution = np.maximum(distribution, min_count)
        
        # Adjust to ensure we distribute exactly all items
        total_distributed = distribution.sum()
        diff = len(items) - total_distributed
        
        if diff > 0:
            # Add remaining items to groups with highest weights (lowest ranks)
            for i in range(diff):
                distribution[i % count] += 1
        elif diff < 0:
            # Remove excess items from groups with lowest weights (highest ranks)
            for i in range(-diff):
                idx = count - 1 - (i % count)
                if distribution[idx] > min_count:
                    distribution[idx] -= 1
        
        return distribution.tolist()
    
    @staticmethod
    def distribute_pods_to_servers(num_servers, pods_per_server, strategy="uniform", **kwargs):
        """
        Distribute pods to servers using the specified strategy
        
        Args:
            num_servers: Number of servers
            pods_per_server: Target number of pods per server
            strategy: Distribution strategy ("uniform", "pareto", "zipf")
            **kwargs: Additional parameters for the distribution
            
        Returns:
            List of pod counts for each server
        """
        total_pods = num_servers * pods_per_server
        dummy_items = list(range(total_pods))
        
        if strategy == "uniform":
            return DistributionStrategies.uniform_distribution(dummy_items, num_servers)
        elif strategy == "pareto":
            alpha = kwargs.get("alpha", 1.5)
            min_count = kwargs.get("min_pods_per_server", 1)
            return DistributionStrategies.pareto_distribution(dummy_items, num_servers, alpha, min_count)
        elif strategy == "zipf":
            alpha = kwargs.get("alpha", 1.2)
            min_count = kwargs.get("min_pods_per_server", 1)
            return DistributionStrategies.zipf_distribution(dummy_items, num_servers, alpha, min_count)
        else:
            raise ValueError(f"Unknown distribution strategy: {strategy}")
    
    @staticmethod
    def distribute_files_to_pods(source_files, pod_counts, strategy="uniform", **kwargs):
        """
        Distribute files to pods using the specified strategy
        
        Args:
            source_files: List of source files
            pod_counts: List of pod counts for each server
            strategy: Distribution strategy ("uniform", "pareto", "zipf")
            **kwargs: Additional parameters for the distribution
            
        Returns:
            Dictionary mapping (server_id, pod_id) to list of files
        """
        total_pods = sum(pod_counts)
        distribution = defaultdict(list)
        
        if strategy == "uniform":
            # Uniform distribution across all pods
            files_per_pod = len(source_files) // total_pods
            remainder = len(source_files) % total_pods
            
            file_index = 0
            for server_id, pod_count in enumerate(pod_counts, 1):
                for pod_id in range(1, pod_count + 1):
                    files_for_pod = files_per_pod + (1 if remainder > 0 else 0)
                    if remainder > 0:
                        remainder -= 1
                    
                    distribution[(server_id, pod_id)] = source_files[file_index:file_index + files_for_pod]
                    file_index += files_for_pod
        
        elif strategy == "pareto":
            # Pareto distribution across all pods
            alpha = kwargs.get("alpha", 1.5)
            min_count = kwargs.get("min_files_per_pod", 1)
            
            # Generate weights for all pods
            weights = np.random.pareto(alpha, total_pods)
            weights = np.maximum(weights, 0.1)
            weights = weights / weights.sum()
            
            # Calculate file distribution
            file_counts = (weights * len(source_files)).astype(int)
            file_counts = np.maximum(file_counts, min_count)
            
            # Adjust to ensure we distribute all files
            total_distributed = file_counts.sum()
            diff = len(source_files) - total_distributed
            
            if diff > 0:
                sorted_indices = np.argsort(weights)[::-1]
                for i in range(diff):
                    file_counts[sorted_indices[i % total_pods]] += 1
            elif diff < 0:
                sorted_indices = np.argsort(weights)
                for i in range(-diff):
                    idx = sorted_indices[i % total_pods]
                    if file_counts[idx] > min_count:
                        file_counts[idx] -= 1
            
            # Distribute files according to calculated counts
            file_index = 0
            pod_index = 0
            for server_id, pod_count in enumerate(pod_counts, 1):
                for pod_id in range(1, pod_count + 1):
                    files_for_pod = file_counts[pod_index]
                    distribution[(server_id, pod_id)] = source_files[file_index:file_index + files_for_pod]
                    file_index += files_for_pod
                    pod_index += 1
        
        elif strategy == "zipf":
            # Zipf distribution across all pods
            alpha = kwargs.get("alpha", 1.2)
            min_count = kwargs.get("min_files_per_pod", 1)
            
            # Generate weights for all pods
            ranks = np.arange(1, total_pods + 1)
            weights = 1 / np.power(ranks, alpha)
            weights = weights / weights.sum()
            
            # Calculate file distribution
            file_counts = (weights * len(source_files)).astype(int)
            file_counts = np.maximum(file_counts, min_count)
            
            # Adjust to ensure we distribute all files
            total_distributed = file_counts.sum()
            diff = len(source_files) - total_distributed
            
            if diff > 0:
                for i in range(diff):
                    file_counts[i % total_pods] += 1
            elif diff < 0:
                for i in range(-diff):
                    idx = total_pods - 1 - (i % total_pods)
                    if file_counts[idx] > min_count:
                        file_counts[idx] -= 1
            
            # Distribute files according to calculated counts
            file_index = 0
            pod_index = 0
            for server_id, pod_count in enumerate(pod_counts, 1):
                for pod_id in range(1, pod_count + 1):
                    files_for_pod = file_counts[pod_index]
                    distribution[(server_id, pod_id)] = source_files[file_index:file_index + files_for_pod]
                    file_index += files_for_pod
                    pod_index += 1
        
        else:
            raise ValueError(f"Unknown distribution strategy: {strategy}")
        
        return distribution
    
    @staticmethod
    def get_distribution_stats(distribution):
        """
        Get statistics about the file distribution
        
        Args:
            distribution: Dictionary mapping (server_id, pod_id) to list of files
            
        Returns:
            Dictionary with distribution statistics
        """
        file_counts = [len(files) for files in distribution.values()]
        pod_counts = []
        
        # Get pod counts per server
        server_pods = defaultdict(list)
        for (server_id, pod_id), files in distribution.items():
            server_pods[server_id].append((pod_id, len(files)))
        
        for server_id, pods in server_pods.items():
            pod_counts.append(len(pods))
        
        return {
            "total_servers": len(set(server_id for server_id, _ in distribution.keys())),
            "total_pods": len(distribution),
            "total_files": sum(file_counts),
            "min_files_per_pod": min(file_counts) if file_counts else 0,
            "max_files_per_pod": max(file_counts) if file_counts else 0,
            "avg_files_per_pod": sum(file_counts) / len(file_counts) if file_counts else 0,
            "std_dev_files": np.std(file_counts) if file_counts else 0,
            "min_pods_per_server": min(pod_counts) if pod_counts else 0,
            "max_pods_per_server": max(pod_counts) if pod_counts else 0,
            "avg_pods_per_server": sum(pod_counts) / len(pod_counts) if pod_counts else 0,
            "std_dev_pods": np.std(pod_counts) if pod_counts else 0
        }