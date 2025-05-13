import ptf.testutils as testutils
from bfruntime_client_base_tests import BfRuntimeTest
import bfrt_grpc.client as gc
import p4testutils.misc_utils as misc_utils

class HashTableCounter:
    def __init__(self):
        self.table = {}

    def insert(self, label, p_idx, value):
        if label in self.table:
            self.table[label][p_idx] += value
        else:
            self.table[label] = [0 for _ in range(75)]
            self.table[label][p_idx] += value

class ControlPlane(BfRuntimeTest):

    def __init__(self):
        self.table = HashTableCounter()

    def setUp(self):
        client_id = 0
        p4_name = "tna_digest"
        BfRuntimeTest.setUp(self, client_id, p4_name)

    def preserve(self):

        digest = self.interface.digest_get()

        data_dict = digest.to_dict()

        recv_label = data_dict["label"]
        recv_p_idx = data_dict["p_idx"]
        recv_value = data_dict["value"]

        self.table.insert(label,p_idx,value)

    # Pattern Detector
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

    # burst detect

    ControlPlane.burst_detect(ControlPlane.table, lamb, h, T)

    # damping detect

    ControlPlane.damping_detect(ControlPlane.table, lamb, h, T)

    # strady detect

    ControlPlane.strady_detect(ControlPlane.table, lamb, h, T_)

    # wave detect

    ControlPlane.wave_detect(ControlPlane.table, lamb1, h, T1, lamb2, T2, lamb3, T_)