
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
    print fname_in,fname_out

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

def fill_type(fname_out):
    f_out = h5py.File(fname_out,'a')
    # binascii.crc32 
    tmp_dir = os.path.basename(fname_out)
    tmp_hash = binascii.crc32(tmp_dir)
    tmp_type = binascii.crc32(tmp_dir.split("_",1)[0])
    print fname_out, "making hash from", tmp_dir

    for g in f_out.keys():
        name = '/'+g+'/'
        rname = name+g+'.runNum'
        # print g,name, rname
        # continue
        ssize = f_out[rname].size
        stype_out = f_out[name+'stype']
        snum_out = f_out[name+'snum']
        old_size=stype_out.size
        # print old_size, ssize
        new_size=old_size+ssize
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

    outfile = copy_format_h5.make_prototype(in_dir, out_dir)

    flist = glob.glob(in_dir + '/*')
    if len(flist)==0:
        print "no files in",in_dir
        sys.exit(-1)

    for f in flist:
        merge_file(f, outfile)

    fill_type(outfile)
