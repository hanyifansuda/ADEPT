# ADEPT

## Introduction
Mining temporal patterns in flow spread is critical for applications such as traffic engineering and anomaly detection, where various dynamic behaviors such as bursts and fluctuations often co-occur and are simultaneously required by different applications, making existing single-target detection lose its effect. However, it is challenging to detect diverse patterns simultaneously: line-rate processing of high-speed networks requires programmable hardware deployment, which imposes strict memory and computation constraints. This makes it difficult to estimate single-period spread values, let alone monitor their long-term evolution to mine different characteristics within a unified framework. In this paper, we present ADEPT, a system that unifies heterogeneous pattern detection through a generalizable quantization model and a two-stage architecture that decouples data collection from pattern detection. We first abstract practical requirements into three fundamental meta-patterns to cover the most essential dynamics of spread in real-world scenarios. Then, ADEPT employs a self-maintaining 2D Twin Bitmap and Record Filter in the data plane to ensure efficient and autonomous data collection, while delegating gradient-based detection to the control plane. We prototype ADEPT on a $6.4$Tbps Tofino switch. Evaluations on real-world traces show that ADEPT achieves up to $6.51 \times$ higher precision, $80.45 \times$ higher recall, and $42.76 \times$ higher $F_1$ score than state-of-the-art solutions, while delivering $182 \times$ throughput on programmable hardware.

## Descriptions

./P4 contains the hardware prototype of ADEPT in P4 16, including both data plane and control plane

./CPU contains the software implementation of ADEPT and other related works

### Dataset:

Due to privacy requirements, we do not have the right to share the data used. Users can operate with their own data.

The recommended data format is as follows:

FlowLabel Element
