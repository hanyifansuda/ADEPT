# ground truth of burst
def burst_GT(lamb, h, T):

    flow_records = {}

    with open("caida_data.txt", "r") as f:
        
        for line in f:
            
            src, dst = line.split(" ")[1], line.split(" ")[2][:-2]

            if src not in flow_records:
                
                flow_records[src] = [0 for _ in range(800)]

    monitor_spread = {}
    count = 0
    epoch = 0

    ground_truth = {}
    details = {}

    with open("caida_data.txt", "r") as f:
        
        for line in f:
            
            count += 1
            
            src, dst = line.split(" ")[1], line.split(" ")[2][:-2]

            if src in monitor_spread:
                monitor_spread[src].add(dst)
            else:
                monitor_spread[src] = set()
                monitor_spread[src].add(dst)
                
            if count % 2000000 == 0:
                
                for key, values in monitor_spread.items():
                    
                    flow_records[key][epoch] = len(values)
                
                if epoch >= 2*h-1:
                
                    for key in monitor_spread.keys():
                    
                        window = flow_records[key][epoch-2*h+1:epoch+1]
                        
                        alpha_dict = {}
                        flag = True
                        for l in range(1,h+1):
                            if flag:
                                if window[-l] < T:
                                    flag = False
                                else:
                                    right = sum(window[-l:])/l
                                    left = max( sum(window[-l-h:-l]), 1 )
                                    if left > 1:
                                        left = left/4
                                    alpha_dict[l] = right/left
                                
                        if len(alpha_dict) != 0:
                            if max(alpha_dict.values()) >= lamb:

                                res = max(alpha_dict, key=lambda x: alpha_dict[x])

                                if key not in ground_truth:
                                    ground_truth[key] = [[epoch-res+1, epoch]]
                                else:
                                    ground_truth[key].append([epoch-res+1, epoch])

                                details[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]
                
                monitor_spread = {}
                
                epoch += 1

    with open("ground_truth_burst","w") as f:
        f.write(str(ground_truth))


# ground truth of damping
def damping_GT(lamb, h, T):

    count = 0
    epoch = 0
    flow_set = {}
    flow_records = {}

    with open("caida_data.txt", "r") as file:

        for line in file:

            count += 1

            f, e = line.split(" ")[1], line.split(" ")[2][:-2]

            # spread calculation
            if f in flow_set:
                flow_set[f].add(e)
            else:
                flow_set[f] = set()
                flow_set[f].add(e)
                
            if count % 2000000 == 0:
                
                for key, values in flow_set.items():
                    if key in flow_records:
                        flow_records[key][epoch] = len(values)
                    else:
                        flow_records[key] = [0 for _ in range(92)]
                        flow_records[key][epoch] = len(values)
                
                epoch += 1

                flow_set = {}

    epoch = 0

    ground_truth = {}
    details = {}

    for epoch in range(92):

        if epoch >= 2*h-1:

            for key in flow_records.keys():

                window = flow_records[key][epoch-2*h+1:epoch+1]

                alpha_dict = {}
                for l in range(1,h+1):
                    left = window[-l-h:-l]
                    right = window[-l:]
                    if min(left) >= T and max(right) < T:
                        right = max(sum(window[-l:]), 1)
                        if right > 1:
                            right = right/l
                        left = sum(window[-l-h:-l])/4
                        alpha_dict[l] = left/right

                if len(alpha_dict) != 0:
                    if max(alpha_dict.values()) >= lamb:

                        res = max(alpha_dict, key=lambda x: alpha_dict[x])

                        if key not in ground_truth:
                            ground_truth[key] = [[epoch-res+1, epoch]]
                        else:
                            ground_truth[key].append([epoch-res+1, epoch])

                        details[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]

    with open("ground_truth_damping","w") as f:
        f.write(str(ground_truth))


# ground truth of steady
def steady_GT(lamb, h, T_):

    def find_closest_key(dictionary, lambda_val):
        closest_key = []
        for key, value in dictionary.items():
            if (1 - lambda_val) <= value <= (1 + lambda_val):
                closest_key.append(key)
        if closest_key:
            return max(closest_key)

    flow_records = {}

    with open("caida_data.txt", "r") as f:
        
        for line in f:
            
            src, dst = line.split(" ")[1], line.split(" ")[2][:-2]

            if src not in flow_records:
                
                flow_records[src] = [0 for _ in range(800)]

    monitor_spread = {}
    count = 0
    epoch = 0

    ground_truth = {}
    details = {}

    with open("caida_data.txt", "r") as f:
        
        for line in f:
            
            count += 1
            
            src, dst = line.split(" ")[1], line.split(" ")[2][:-2]

            if src in monitor_spread:
                monitor_spread[src].add(dst)
            else:
                monitor_spread[src] = set()
                monitor_spread[src].add(dst)
                
            if count % 2000000 == 0:
                
                for key, values in monitor_spread.items():
                    
                    flow_records[key][epoch] = len(values)
                
                if epoch >= 2*h-1:
                
                    for key in monitor_spread.keys():
                    
                        window = flow_records[key][epoch-2*h+1:epoch+1]

                        alpha_dict = {}
                        for l in range(1,h+1):
                            if np.mean(window[-l-h:]) > T_:
                                left = window[-l-h:-l]
                                right = window[-l:]
                                right = max(sum(window[-l:]), 1)/l
                                left = sum(window[-l-h:-l])/4
                                alpha_dict[l] = left/right

                        if len(alpha_dict) != 0:

                            res = find_closest_key(alpha_dict, lamb)

                            if res:

                                if key not in ground_truth:
                                    ground_truth[key] = [[epoch-res+1, epoch]]
                                else:
                                    ground_truth[key].append([epoch-res+1, epoch])

                                details[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]
                
                monitor_spread = {}
                
                epoch += 1

    with open("ground_truth_steady","w") as f:
        f.write(str(ground_truth))