
import h5py
import sys
import binascii
import os
import glob
import copy_format_h5

# adds one file at a time to a file that will contain everything

# in_dir = '/scratch/ssehrish/h5out/TTJets_13TeV_amcatnloFXFX_pythia8_2'
# out_dir = '/scratch2/jbk'

# fname_out = out_dir + '/' + 'TTJets_13TeV_amcatnloFXFX_pythia8_2_all.h5'
# fname_in = in_dir + '/' + 'TTJets_13TeV_amcatnloFXFX_pythia8_2_0.h5'

def merge_file(fname_in, fname_out):
    f_in = h5py.File(fname_in,'r')
    f_out = h5py.File(fname_out,'a')

    for g in f_in.keys():
        print g
        # still need to generate the values within this group
        for d in f_in['/'+g].keys():
            name = '/'+g+'/'+d
            dset_src = f_in[name]
            dset_dst = f_out[name]
            #print name, dset_dst.size, dset_src.size,dset_dst.shape
            old_size = dset_dst.size
            new_size = dset_dst.size+dset_src.size
            dset_dst.resize( (new_size,) )
            dset_dst[old_size:new_size] = dset_src
            #print name, dset_dst.size, dset_dst.shape

    f_in.close()
    f_out.close()

def fill_type(outfile):
    f_out = h5py.File(fname_out,'a')
    # binascii.crc32 
    tmp_dir = os.path.basename(os.path.dirname(fname_in))
    tmp_hash = binascii.crc32(tmp_dir)
    tmp_type = binascii.crc32(tmp.dir.split("_",1)[0])

    for g in f_in.keys():
        print g
        name = '/'+g+'/'
        ssize = f_in[name+'runNum'].size
        stype_out = f_out[name+'stype']
        snum_out = f_out[name+'snum']
        old_size=stype_out.size
        new_size=stype_out+ssize
        stype_out.resize( (new_size,) )
        snum_out.resize( (new_size,) )
        stype_out[old_size:new_size] = tmp_type
        snum_out[old_size:new_size] = tmp_hash

    f_out.close()

    
if __name__ == '__main__':
	if len(sys.argv)<3: 
		print "wrong arguments, give in_dir and out_dir (in that order)"
		sys.exit(-1)

    in_dir = sys.argv[1]
    out_dir = sys.argv[2]

    outfile = copy_format_h5.make_prototype(in_dir)
    
    flist = glob.glob(in_dir + '/*')
    if len(flist)==0:
        print "no files in",in_dir
        sys.exit(-1)

    for f in flist:
        merge_file(f, outfile)

    fill_type(outfile)
