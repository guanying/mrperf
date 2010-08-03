
#chunk_size = 64

freq_table = {'Xeon 3.2Ghz': 3.2*1024*1024*1024,
			'Xeon 3.0Ghz': 3.0*1024*1024*1024,
			'Xeon 2.5GHz L5420': 2.5*1000*1000*1000,
			}

read_bw_table = {'Seagate': 280.0*1024*1024, \
				}
write_bw_table = {'Seagate': 75.0*1024*1024, \
				}

int_bw = '1Gb'
int_latency = '0.15ms'
ext_bw = '8Gb'
ext_latency = '0.05ms'

rate = 1
maxtime = 1
app_list = [['Terasort', []],
			['Search', [[4, 400]]],
			['Compute', [[400, 4000]]],
			['Index', [[0.02, 0.5]]]]

