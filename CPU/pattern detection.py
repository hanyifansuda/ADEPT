import random
import mmh3
import numpy as np
import math


def ADEPT(M_B, M_DET):

    bitmap_row = (M_B*1024*8)//(2*2*8)
    bitmap1 = [[0,0] for _ in range(bitmap_row)]
    bitmap2 = [[0,0] for _ in range(bitmap_row)]
    
    num_bucket = (M_DET*1024*8)//(32+8+32)
    
    print("num of buckets: "+ str(num_bucket))
    
    # stage 1
    stage1 = []
    for i in range(num_bucket):
        bucket = [None, 0, 0] # f, n_t, epoch
        stage1.append(bucket)
    
    print("total memory usage: "+ str(M_B+M_DET))
    
    count = 0

    epoch = 0
    
    records = {}
    
    with open("caida_data.txt", "r") as file:

        for line in file:

            count += 1
            
            f, e = line.split(" ")[1], line.split(" ")[2]
            
            # duplicate filter
            virtual_element = f + " " +e
            
            hash1 = mmh3.hash(virtual_element, seed = 2, signed=False) % (bitmap_row*8)
            hh = hash1 >> 3
            hh_ = hash1 & 0x07
            hash2 = mmh3.hash(virtual_element, seed = 3, signed=False) % (bitmap_row*8)
            hhh = hash2 >> 3
            hhh_ = hash2 & 0x07
            
            flag1 = False
            if not (bitmap1[hh][epoch%2] & (1 << hh_)):
                bitmap1[hh][epoch%2] |= (1 << hh_)
            else:
                flag1 = True
            bitmap1[hh][(epoch+1)%2] = 0
            
            flag2 = False
            if not (bitmap2[hhh][epoch%2] & (1 << hhh_)):
                bitmap2[hhh][epoch%2] |= (1 << hhh_)
            else:
                flag2 = True
            bitmap2[hhh][(epoch+1)%2] = 0
            
            # records filter
            if not (flag1 and flag2):
                
                # stage1
                sketch_idx1 = mmh3.hash( f , seed = 999 + 1, signed=False) % num_bucket
                
                if stage1[sketch_idx1][0] == f:
                    
                    if stage1[sketch_idx1][1] == epoch:
                    
                        stage1[sketch_idx1][2] += 1
                    
                    else:
                        if stage1[sketch_idx1][2] >= 2:
                        
                            if f in records:
                                records[f][stage1[sketch_idx1][1]] += stage1[sketch_idx1][2]
                            else:
                                records[f] = [0 for _ in range(75)]
                                records[f][stage1[sketch_idx1][1]] = stage1[sketch_idx1][2]
                                
                        else:
                            if random.random() < 0.5:

                                if f in records:
                                    records[f][stage1[sketch_idx1][1]] += stage1[sketch_idx1][2]
                                else:
                                    records[f] = [0 for _ in range(75)]
                                    records[f][stage1[sketch_idx1][1]] = stage1[sketch_idx1][2]
                                
                        stage1[sketch_idx1] = [f, epoch, 1]
                        
                elif stage1[sketch_idx1][0] == None:
                    
                    stage1[sketch_idx1] = [f, epoch, 1]
                    
                else:
                    
                    tempf = stage1[sketch_idx1][0]
                    tempt = stage1[sketch_idx1][1]
                    tempv = stage1[sketch_idx1][2]
                    
                    # replace
                    if stage1[sketch_idx1][1] != epoch:
                        
                        if tempf in records:
                            records[tempf][tempt] += tempv
                        else:
                            records[tempf] = [0 for _ in range(75)]
                            records[tempf][tempt] = tempv

                        stage1[sketch_idx1] = [f, epoch, 1]
                        
                    else:
                        if tempf in records:
                            records[tempf][tempt] += tempv
                        else:
                            records[tempf] = [0 for _ in range(75)]
                            records[tempf][tempt] = tempv
                        
                        stage1[sketch_idx1] = [f, epoch, 1]
                            
            if count % 2000000 == 0:
                
                epoch += 1

    return records


def SpreadSketch(MEMORY, r, b, b_, c, h):

    def find_leftmost0(string, d):
        idx = 0
        string = string[::-1]
        for i in range(len(string)):
            idx += 1
            if string[i] != "0":
                break
        return idx if idx < d else d-1

    # bucket structure
    V_size = math.ceil(b)*(c-1) + math.ceil(b_) # distinct_count size
    K_size = 32 # ID
    L_size = 5
    
    w = (MEMORY*1024*8)//(r*(V_size+K_size+L_size)) # num of buckets
    
    print("num of buckets: "+ str(w))
    
    sketch = []
    for __ in range(r):
        line = []
        for ___ in range(w): # bucket
            mrb = []
            for i in range(c-1):
                line_ = []
                for j in range(b):
                    line_.append(0)
                mrb.append(line_)
            mrb.append( [0 for ii in range(b_)] )
            line.append( [mrb,None,0] )
        sketch.append(line)
        
    overflow_list = []
    for _ in range(2*h):
        lst = set()
        overflow_list.append(lst)
    
    Total_M = MEMORY + (2*h*3000*32)/(1024*8)
    
    print("total memory usage: "+ str(Total_M))
    
    count = 0
    epoch = 0
    
    records = {}
    
    with open("caida_data.txt", "r") as file:

        for line in file:
            
            count += 1

            f, e = line.split(" ")[1], line.split(" ")[2][:-2]
            
            random_binary = mmh3.hash( f+e , seed = 777, signed=False) % 0xffffffff
            level = find_leftmost0(bin(random_binary)[2:], 32)-1
            
            min_value = 0xffffffff
            for i in range(r):
                bucket_idx = mmh3.hash( f , seed = i, signed=False) % w
                # update V
                if level < c-1:
                    p = random_binary % b
                    sketch[i][bucket_idx][0][level][p] = 1
                else:
                    p = random_binary % b_
                    sketch[i][bucket_idx][0][c-1][p] = 1
                # update K & L
                if level >= sketch[i][bucket_idx][2]:
                    sketch[i][bucket_idx][1] = f
                    sketch[i][bucket_idx][2] = level
                
            # detect and refresh
            if count % 2000000 == 0:
                
                flow_list = {}
                
                for i in range(r):
                    for _ in range(w):

                        # estimate
                        base = c-2
                        set_max = b*(1-math.e**(-2.6744))
                        while base > 0 and sketch[i][_][0][base].count(1) <= set_max:
                            base -= 1
                        base += 1
                        m = 0
                        for j in range(base-1,c-1):
                            if sketch[i][_][0][j].count(0) != 0:
                                m += b*math.log(b/(sketch[i][_][0][j].count(0)))
                        if sketch[i][_][0][c-1].count(0) != 0:
                            m += b_*math.log(b_/(sketch[i][_][0][c-1].count(0)))
                        factor = 2 ** (base-1)
                        estimate = factor * m
       
                        flow = sketch[i][_][1]
                        if flow in flow_list:

                            flow_list[flow] = min(estimate, flow_list[flow])

                        else:

                            flow_list[flow] = estimate                   
                
                for key, value in flow_list.items():
                
                    if key in records:
                        records[key][epoch] += value
                    else:
                        records[key] = [0 for _ in range(93)]
                        records[key][epoch] = value            
                
                # refresh
                sketch = []
                for __ in range(r):
                    line = []
                    for ___ in range(w): # bucket
                        mrb = []
                        for i in range(c-1):
                            line_ = []
                            for j in range(b):
                                line_.append(0)
                            mrb.append(line_)
                        mrb.append( [0 for ii in range(b_)] )
                        line.append( [mrb,None,0] )
                    sketch.append(line)

                epoch += 1
            
    return records


def HLLsampler(m, d, w, h):

    def find_leftmost0(string, d):
        idx = 0
        string = string[::-1]
        for i in range(len(string)):
            idx += 1
            if string[i] != "0":
                break            
        return idx if idx < d else d-1


    # bucket structure
    C = []
    for _ in range(d):
        line = []
        for __ in range(w):
            bucket = [1,[0 for ___ in range(m)]]
            line.append(bucket)
        C.append(line)
    M_C = (32+5*m)*(d*w)*1
    
    K = []
    for _ in range(d):
        line = []
        for __ in range(w):
            bucket = None
            line.append(bucket)
        K.append(line)
    M_K = (32)*(d*w)*(2*h)
    
    V = []
    for _ in range(d):
        line = []
        for __ in range(w):
            bucket = 0
            line.append(bucket)
        V.append(line)
    M_V = (32)*(d*w)*(2*h)
    
    Total_M = (M_C + M_K + M_V)/(1024*8)
    
    print("total memory usage: "+ str(Total_M))
    
    count = 0
    epoch = 0
    
    records = {}
    
    with open("caida_data.txt", "r") as file:

        for line in file:
            
            count += 1

            f, e = line.split(" ")[1], line.split(" ")[2][:-2]
            
            for i in range(d):
                
                bucket_idx = mmh3.hash( f , seed = i, signed=False) % w
                
                reg_idx = mmh3.hash( e , seed = 888, signed=False) % m
                
                random_binary = bin(mmh3.hash( e , seed = 777, signed=False) % 0xffffffff)[2:]
                rho = find_leftmost0(random_binary, 32)
            
                if C[i][bucket_idx][1][reg_idx] < rho:
                    
                    p_ = C[i][bucket_idx][0]
                    
                    C[i][bucket_idx][0] += \
                    - (1/m) * (1/(2**C[i][bucket_idx][1][reg_idx])) + (1/m) * (1/(2**rho))
                    
                    C[i][bucket_idx][1][reg_idx] = rho
                    
                    # Case 1
                    if K[i][bucket_idx] == None:
                         
                        K[i][bucket_idx] = f
                            
                        V[i][bucket_idx] = 1/p_
                        
                    # Case 2
                    elif K[i][bucket_idx] == f:
                        
                        V[i][bucket_idx] += 1/p_
                        
                    # Case 3
                    else:
                        
                        pd = 1.08**(-V[i][bucket_idx])
                        
                        if random.random() < pd:
                            
                            if random.random() < (1/p_)/(math.ceil(1/p_)):
                                
                                V[i][bucket_idx] -= math.ceil(1/p_)
                                
                                if V[i][bucket_idx] < 0:
                                    
                                    K[i][bucket_idx] = f
                                    
                                    V[i][bucket_idx] *= -1
                
            # detect and refresh
            if count % 2000000 == 0:
                
                flow_list = {}

                for i in range(d):

                    for j in range(w):

                        flow = K[i][j]
                        value = V[i][j]

                        if flow in flow_list:

                            flow_list[flow] = max(value, flow_list[flow])

                        else:

                            flow_list[flow] = value                
                
                for key, value in flow_list.items():
                
                    if key in records:
                        records[key][epoch] += value
                    else:
                        records[key] = [0 for _ in range(93)]
                        records[key][epoch] = value                
                
                # refresh
                C = []
                for _ in range(d):
                    line = []
                    for __ in range(w):
                        bucket = [1,[0 for ___ in range(m)]]
                        line.append(bucket)
                    C.append(line)

                K = []
                for _ in range(d):
                    line = []
                    for __ in range(w):
                        bucket = None
                        line.append(bucket)
                    K.append(line)

                V = []
                for _ in range(d):
                    line = []
                    for __ in range(w):
                        bucket = 0
                        line.append(bucket)
                    V.append(line)

                epoch += 1
            
    return records


def burst_detect(records, lamb, h, T):

    epoch = 0

    detection = {}
    details = {}

    for epoch in range(75):

        if epoch >= 2*h-1:

            for key in records.keys():

                window = records[key][epoch-2*h+1:epoch+1]
                
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
                            alpha_dict[l] = (right)/(left)

                if len(alpha_dict) != 0:
                    if max(alpha_dict.values()) >= lamb:

                        res = max(alpha_dict, key=lambda x: alpha_dict[x])

                        if key not in detection:
                            detection[key] = [[epoch-res+1, epoch]]
                        else:
                            detection[key].append([epoch-res+1, epoch])

                        details[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]

    with open("ground_truth_burst","r") as f:
        ground_truth = eval(f.readline())

    # PR
    correct_report = 0
    for key in detection.keys():
        for period in detection[key]:
            if key in ground_truth:
                if period in ground_truth[key]:
                    correct_report += 1
    PR = correct_report/sum(len(value) for value in detection.values())
    print("PR = " + str(PR))

    # RR
    RR = correct_report/sum(len(value) for value in ground_truth.values())
    print("RR = " + str(RR))

    # F1-score
    print("F1-score = " + str( (2*PR*RR)/(PR+RR) ))


def damping_detect(records, lamb, h, T):

    epoch = 0

    detection = {}
    details = {}

    for epoch in range(75):

        if epoch >= 2*h-1:

            for key in records.keys():

                window = records[key][epoch-2*h+1:epoch+1]

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

                        if key not in detection:
                            detection[key] = [[epoch-res+1, epoch]]
                        else:
                            detection[key].append([epoch-res+1, epoch])

                        details[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]

    with open("ground_truth_damping","r") as f:
        ground_truth = eval(f.readline())

    # PR
    correct_report = 0
    for key in detection.keys():
        for period in detection[key]:
            if key in ground_truth:
                if period in ground_truth[key]:
                    correct_report += 1
    PR = correct_report/sum(len(value) for value in detection.values())
    print("PR = " + str(PR))

    # RR
    RR = correct_report/sum(len(value) for value in ground_truth.values())
    print("RR = " + str(RR))

    # F1-score
    print("F1-score = " + str( (2*PR*RR)/(PR+RR) ))


def steady_detect(records, lamb, h, T_):

    epoch = 0

    detection = {}
    details = {}

    def find_closest_key(dictionary, lambda_val):
        closest_key = []
        for key, value in dictionary.items():
            if (1 - lambda_val) <= value <= (1 + lambda_val):
                closest_key.append(key)
        if closest_key:
            return max(closest_key)

    for epoch in range(75):

        if epoch >= 2*h-1:

            for key in records.keys():

                window = records[key][epoch-2*h+1:epoch+1]

                alpha_dict = {}
                for l in range(1,h+1):
                    if sum(window[-l-h:])/(h+l) > T_:
                        left = window[-l-h:-l]
                        right = window[-l:]
                        right = max(sum(window[-l:]), 1)/l
                        left = sum(window[-l-h:-l])/4
                        alpha_dict[l] = left/right

                if len(alpha_dict) != 0:

                    res = find_closest_key(alpha_dict, lamb)

                    if res:

                        if key not in detection:
                            detection[key] = [[epoch-res+1, epoch]]
                        else:
                            detection[key].append([epoch-res+1, epoch])

                        details[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]


    with open("ground_truth_steady","r") as f:
        ground_truth = eval(f.readline())

    # PR
    correct_report = 0
    for key in detection.keys():
        for period in detection[key]:
            if key in ground_truth:
                if period in ground_truth[key]:
                    correct_report += 1
    PR = correct_report/sum(len(value) for value in detection.values())
    print("PR = " + str(PR))

    # RR
    RR = correct_report/sum(len(value) for value in ground_truth.values())
    print("RR = " + str(RR))

    # F1-score
    print("F1-score = " + str( (2*PR*RR)/(PR+RR) ))


def wave_detect(records, lamb1, h, T1, lamb2, T2, lamb3, T_):

    def find_closest_key(dictionary, lambda_val):
        closest_key = []
        for key, value in dictionary.items():
            if (1 - lambda_val) <= value <= (1 + lambda_val):
                closest_key.append(key)
        if closest_key:
            return max(closest_key)

    ground_truth_burst = dict()
    ground_truth_damp = dict()
    ground_truth_steady = dict()

    details_burst = dict()
    details_damp = dict()
    details_steady = dict()

    for key in records.keys():

        for epoch in range(74):

            if epoch >= 2*h-1:

                window = records[key][epoch-2*h+1:epoch+1]
                
                # burst detect
                alpha_dict = {}
                flag = True
                for l in range(1,h+1):
                    if flag:
                        if window[-l] < T1:
                            flag = False
                        else:
                            right = sum(window[-l:])/l
                            left = max( sum(window[-l-h:-l]), 1 )
                            if left > 1:
                                left = left/4
                            alpha_dict[l] = right/left

                if len(alpha_dict) != 0:
                    if max(alpha_dict.values()) >= lamb1:

                        res = max(alpha_dict, key=lambda x: alpha_dict[x])

                        if key not in ground_truth_burst:
                            ground_truth_burst[key] = [[epoch-res+1, epoch]]
                        else:
                            ground_truth_burst[key].append([epoch-res+1, epoch])

                        details_burst[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]
                
                
                # damp detect
                alpha_dict = {}
                for l in range(1,h+1):
                    left = window[-l-h:-l]
                    right = window[-l:]
                    if min(left) >= T2 and max(right) < T2:
                        right = max(sum(window[-l:]), 1)
                        if right > 1:
                            right = right/l
                        left = sum(window[-l-h:-l])/4
                        alpha_dict[l] = left/right

                if len(alpha_dict) != 0:
                    if max(alpha_dict.values()) >= lamb2:

                        res = max(alpha_dict, key=lambda x: alpha_dict[x])

                        if key not in ground_truth_damp:
                            ground_truth_damp[key] = [[epoch-res+1, epoch]]
                        else:
                            ground_truth_damp[key].append([epoch-res+1, epoch])

                        details_damp[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]
                        
                # steady detect
                alpha_dict = {}
                for l in range(1,h+1):
                    if sum(window[-l-h:])/(h+l) > T_:
                        left = window[-l-h:-l]
                        right = window[-l:]
                        right = max(sum(window[-l:]), 1)/l
                        left = sum(window[-l-h:-l])/4
                        alpha_dict[l] = left/right

                if len(alpha_dict) != 0:

                    res = find_closest_key(alpha_dict, lamb3)

                    if res:

                        if key not in ground_truth_steady:
                            ground_truth_steady[key] = [[epoch-res+1, epoch]]
                        else:
                            ground_truth_steady[key].append([epoch-res+1, epoch])

                        details_steady[key+" "+str([epoch-res+1, epoch])] = [window, window[-res:]]
                        
                        
    def find_all_abc_sequences(dict_A, dict_B, dict_C, dura=4):
        results = {}

        common_ids = set(dict_A) & set(dict_B) & set(dict_C)

        for obj_id in common_ids:
            abc_sequences = []
            periods_A = dict_A[obj_id]
            periods_B = dict_B[obj_id]
            periods_C = dict_C[obj_id]

            for a_start, a_end in periods_A:
                for b_start, b_end in periods_B:
                    if a_end <= b_start <= a_end + dura:
                        for c_start, c_end in periods_C:
                            if b_end <= c_start <= b_end + dura:
                                abc_sequences.append({
                                    'A': [a_start, a_end],
                                    'B': [b_start, b_end],
                                    'C': [c_start, c_end]
                                })

            if abc_sequences:
                results[obj_id] = abc_sequences

        return results

    wave_res = find_all_abc_sequences(ground_truth_burst,ground_truth_damp,ground_truth_steady)

    with open("ground_truth_wave","r") as f:
        ground_truth = eval(f.readline())

    def flatten_sequences(data):
        flat = []
        for obj_id, sequences in data.items():
            for seq in sequences:
                flat.append((obj_id,
                            tuple(seq['A']),
                            tuple(seq['B']),
                            tuple(seq['C'])))
        return flat

    #############################################
    gt_flat = flatten_sequences(ground_truth)
    test_flat = flatten_sequences(wave_res)

    matched_gt = set()
    true_positives = 0

    for seq in test_flat:
        if seq in gt_flat:
            idx = gt_flat.index(seq)
            if idx not in matched_gt:
                true_positives += 1
                matched_gt.add(idx)

    precision = true_positives / len(test_flat) if test_flat else 0
    recall = true_positives / len(gt_flat) if gt_flat else 0
    if precision + recall == 0:
        f1 = 0
    else:
        f1 = 2 * precision * recall / (precision + recall)

    print("PR = " + str(precision))
    print("RR = " + str(recall))
    print("F1-score = " + str( f1 ))


if __name__ == '__main__':

    MEMORY = 2048

    records = ADEPT(M_B = int(MEMORY//(1+8)), M_DET = int(MEMORY - MEMORY//(1+8)))
    # records = SpreadSketch(MEMORY, r, b, b_, c, h)
    # records = HLLsampler(MEMORY, d, w, h)

    # burst detect

    burst_detect(records, lamb, h, T)

    # damping detect

    damping_detect(records, lamb, h, T)

    # strady detect

    strady_detect(records, lamb, h, T_)

    # wave detect

    wave_detect(records, lamb1, h, T1, lamb2, T2, lamb3, T_)