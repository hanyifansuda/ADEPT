#include <core.p4>
#include <tna.p4>

const bit<16> TYPE_IPV4 = 0x800;
const bit<16> TYPE_ETHER = 0x6558;
const bit<8> TYPE_TCP = 6;
const bit<8> TYPE_UDP = 17;

#define BITMAP_LEN 65536
#define BUCKET_LEN 65536

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

typedef bit<9>  egressSpec_t;
typedef bit<48> macAddr_t;
typedef bit<32> ip4Addr_t;

header ethernet_t {
    macAddr_t dstAddr;
    macAddr_t srcAddr;
    bit<16>   etherType;
}

header ipv4_t {
    bit<4>    version;
    bit<4>    ihl;
    bit<8>    diffserv;
    bit<16>   totalLen;
    bit<16>   identification;
    bit<3>    flags;
    bit<13>   fragOffset;
    bit<8>    ttl;
    bit<8>    protocol;
    bit<16>   hdrChecksum;
    bit<32>   srcAddr;
    bit<32>   dstAddr;
}

header tcp_t {
    bit<16>     srcPort;
    bit<16>     dstPort;
    bit<32>     seqNo;
    bit<32>     ackNo;
    bit<4>      dataOffset;
    bit<4>      res;
    bit<8>      flags;
    bit<16>     windows;
    bit<16>     checksum;
    bit<16>     urgenPtr;

}

struct headers {
    ethernet_t   out_ethernet;
    ipv4_t       ipv4;
    tcp_t        tcp;
}


//////////////////* Metadata *//////////////////
//////////////////* Metadata *//////////////////
//////////////////* Metadata *//////////////////

struct metadata_t {
    bit<64> ve;

    bit<19> hash_res1;
    bit<16> row_idx1;
    bit<3> col_idx1;
    bit<8> col_idx12;

    bit<19> hash_res2;
    bit<16> row_idx2;
    bit<3> col_idx2;
    bit<8> col_idx22;

    bit<8> res1;
    bit<8> res2;

    bit<32> rnum1;
    bit<32> rnum2;

    bit<1> sample_flag;

    bit<16> epoch;

    bit<32> read_label;
    bit<16> read_epoch;
    bit<16> read_counter;
}

struct digest_t{
	bit<32> d_label;
    bit<16> d_epoch;
    bit<16> d_counter;
}


/*************************************************************************
*********************** P A R S E R  ***********************************
*************************************************************************/

parser SwitchIngressParser(
                packet_in packet,
                out headers hdr,
	            out metadata_t meta,
                out ingress_intrinsic_metadata_t ig_intr_md) {

    state start {
        packet.extract(ig_intr_md);
        transition select(ig_intr_md.resubmit_flag) {
            1 : parse_resubmit;
            0 : parse_port_metadata;
        }
    }

    state parse_resubmit {
        transition parse_out_ethernet;
    }

    state parse_port_metadata {
        packet.advance(PORT_METADATA_SIZE);
        transition parse_out_ethernet;
    }

    state parse_out_ethernet {
        packet.extract(hdr.out_ethernet);
        transition select(hdr.out_ethernet.etherType) {
            TYPE_IPV4: parse_ipv4;
            default: accept;
        }
    }

    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol){
            TYPE_TCP: parse_tcp;
        }
    }

    state parse_tcp {
        packet.extract(hdr.tcp);
        transition accept;
    }
}




/*************************************************************************
**************  I N G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

control SwitchIngress(
        inout headers hdr,
        inout metadata_t meta,
        in ingress_intrinsic_metadata_t ig_intr_md,
        in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_tm_md
        )
{

    CRCPolynomial<bit<32>>(coeff=0x04C11DB7, reversed=true, msb=false, extended=false, init=0xFFFFFFFF, xor=0xFFFFFFFF) crc32_1;
	
    Hash<bit<19>>(HashAlgorithm_t.CUSTOM, crc32_1) hash1;
	
    CRCPolynomial<bit<32>>(coeff=0x04C11DB7, reversed=true, msb=false, extended=false, init=0xFFFFFFFF, xor=0x00000000) crc32_2;

	Hash<bit<19>>(HashAlgorithm_t.CUSTOM, crc32_2) hash2;

	Register<bit<8>,bit<16>>(BITMAP_LEN) bitmap11;

	RegisterAction<bit<8>,bit<16>,bit<8>>(bitmap11) bitmap_alu11_set={
		void apply(inout bit<8> reg, out bit<8> output){
			
            output = reg;

            reg = reg | meta.col_idx12;

		}
	};

	RegisterAction<bit<8>,bit<16>,bit<8>>(bitmap11) bitmap_alu11_reset={
		void apply(inout bit<8> reg, out bit<8> output){
			
            reg = 0;

		}
	};

	Register<bit<8>,bit<16>>(BITMAP_LEN) bitmap12;

	RegisterAction<bit<8>,bit<16>,bit<8>>(bitmap12) bitmap_alu12_set={
		void apply(inout bit<8> reg, out bit<8> output){
			
            output = reg;

            reg = reg | meta.col_idx12;

		}
	};

	RegisterAction<bit<8>,bit<16>,bit<8>>(bitmap12) bitmap_alu12_reset={
		void apply(inout bit<8> reg, out bit<8> output){
			
            reg = 0;

		}
	};

	Register<bit<8>,bit<16>>(BITMAP_LEN) bitmap21;

	RegisterAction<bit<8>,bit<16>,bit<8>>(bitmap21) bitmap_alu21_set={
		void apply(inout bit<8> reg, out bit<8> output){

            output = reg;

            reg = reg | meta.col_idx22;

		}
	};

	RegisterAction<bit<8>,bit<16>,bit<8>>(bitmap21) bitmap_alu21_reset={
		void apply(inout bit<8> reg, out bit<8> output){

            reg = 0;

		}
	};

	Register<bit<8>,bit<16>>(BITMAP_LEN) bitmap22;

	RegisterAction<bit<8>,bit<16>,bit<8>>(bitmap22) bitmap_alu22_set={
		void apply(inout bit<8> reg, out bit<8> output){

            output = reg;

            reg = reg | meta.col_idx22;

		}
	};

	RegisterAction<bit<8>,bit<16>,bit<8>>(bitmap22) bitmap_alu22_reset={
		void apply(inout bit<8> reg, out bit<8> output){

            reg = 0;

		}
	};

	action set_level1(bit<8> level) {
		meta.col_idx12 = level;
	}
	table cal_level1 {
		key = {
			meta.col_idx1 : exact;
		}
		actions = {
			set_level1;
		}
	}

	action set_level2(bit<8> level) {
		meta.col_idx22 = level;
	}
	table cal_level2 {
		key = {
			meta.col_idx2 : exact;
		}
		actions = {
			set_level2;
		}
	}

    CRCPolynomial<bit<32>>(coeff=0x04C11DB7, reversed=true, msb=false, extended=false, init=0xFFFFFFFF, xor=0x12345678) crc32_3;
	
    Hash<bit<16>>(HashAlgorithm_t.CUSTOM, crc32_3) hash3;

	Register<bit<32>,bit<16>>(BUCKET_LEN) label_array;

	RegisterAction<bit<32>,bit<16>,bit<32>>(label_array) label_array_alu={
		void apply(inout bit<32> reg, out bit<32> output){
            
            output = reg;

            reg = hdr.ipv4.srcAddr;

		}
	};

    Register<bit<16>,bit<16>>(BUCKET_LEN) epoch_array;

	RegisterAction<bit<16>,bit<16>,bit<16>>(epoch_array) epoch_array_alu={
		void apply(inout bit<16> reg, out bit<16> output){

            output = reg;

            reg = meta.epoch;

		}
	};

    Register<bit<16>,bit<16>>(BUCKET_LEN) counter_array;

	RegisterAction<bit<16>,bit<16>,bit<16>>(counter_array) counter_array_alu1={
		void apply(inout bit<16> reg, out bit<16> output){

            output = reg;

            reg = 1;

		}
	};

	RegisterAction<bit<16>,bit<16>,bit<16>>(counter_array) counter_array_alu2={
		void apply(inout bit<16> reg, out bit<16> output){

            output = reg;

            reg = reg + 1;

		}
	};

    Random<bit<8>>() random_gen;

// apply

    apply{
        
        meta.hash_res1 = hash1.get({hdr.ipv4.srcAddr,hdr.ipv4.dstAddr});
        meta.row_idx1 = (bit<16>) (meta.hash_res1 [18:3]);
        meta.col_idx1 = (bit<3>) (meta.hash_res1 [2:0]);
        cal_level1.apply();

        meta.hash_res2 = hash2.get({hdr.ipv4.srcAddr,hdr.ipv4.dstAddr});
        meta.row_idx2 = (bit<16>) (meta.hash_res2 [18:3]);
        meta.col_idx2 = (bit<3>) (meta.hash_res2 [2:0]);
        cal_level2.apply();

        if (meta.epoch & 1 == 0){
            meta.res1 = bitmap_alu11_set.execute(meta.row_idx1);
            bitmap_alu12_reset.execute(meta.row_idx1);
            meta.res2 = bitmap_alu21_set.execute(meta.row_idx2);
            bitmap_alu22_reset.execute(meta.row_idx2);
        }
        else{
            meta.res1 = bitmap_alu12_set.execute(meta.row_idx1);
            bitmap_alu11_reset.execute(meta.row_idx1);
            meta.res2 = bitmap_alu22_set.execute(meta.row_idx2);
            bitmap_alu21_reset.execute(meta.row_idx2);            
        }

        meta.res1 = meta.res1 | meta.col_idx12;
        meta.res2 = meta.res2 | meta.col_idx22;
        bit<16> idx = hash3.get(hdr.ipv4.srcAddr);
        bit<8> random_num = random_gen.get();
        bit<1> flag = 0;
        if (random_num > 128){
            flag = 1;
        }

        if (meta.res1 == 1 && meta.res2 == 1) {
            meta.read_label = label_array_alu.execute(idx);
            meta.read_epoch = epoch_array_alu.execute(idx);
            meta.sample_flag = 1;

            if (meta.read_label == hdr.ipv4.srcAddr) {
                if (meta.read_epoch == meta.epoch) {
                    meta.read_counter = counter_array_alu2.execute(idx);
                }
                else {
                    meta.read_counter = counter_array_alu1.execute(idx);
                    if (meta.read_counter == 1 && flag == 1) {
                        meta.sample_flag = 0;
                    }
                }
            }
            else{
                meta.read_counter = counter_array_alu1.execute(idx);
            }
        }
    }
}

/*************************************************************************
***********************  D E P A R S E R  *******************************
*************************************************************************/

control SwitchIngressDeparser(
        packet_out packet,
        inout headers hdr,
        in metadata_t meta,
        in ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md) {
    
	Digest<digest_t>() digest;
	apply{
		if (meta.sample_flag == 1) {
			digest.pack({meta.read_label,meta.read_epoch,meta.read_counter});
		}
		packet.emit(hdr);
	}
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

parser EgressParser(
	packet_in packet,
	out headers hdr,
	out metadata_t meta,
	out egress_intrinsic_metadata_t eg_intr_md){

	state start {
 		packet.extract(eg_intr_md);
        transition accept;
	}
   
}

control Egress(
	inout headers hdr,
	inout metadata_t meta,
	in egress_intrinsic_metadata_t eg_intr_md,
	in egress_intrinsic_metadata_from_parser_t eg_prsr_md,
	inout egress_intrinsic_metadata_for_deparser_t eg_dprsr_md,
	inout egress_intrinsic_metadata_for_output_port_t eg_intr_md_for_oport){

	apply{}

}

control EgressDeparser(
	packet_out packet,
	inout headers hdr,
	in metadata_t eg_md,
	in egress_intrinsic_metadata_for_deparser_t eg_dprsr_md) {

	apply{
        packet.emit(hdr);
    }
}


/*************************************************************************
***********************  S W I T C H  *******************************
*************************************************************************/

Pipeline(SwitchIngressParser(),
         SwitchIngress(),
         SwitchIngressDeparser(),
         EgressParser(),
         Egress(),
         EgressDeparser()) pipe;

Switch(pipe) main;