#!/home/puzheng/anaconda3/bin/python

import os, sys, getopt, re

def parse_arguments(argv):
    # defaults
    source_folder = '.'
    re_string = r'Conv_zscan_(?P<fov>[0-9]+)\.([0-9a-z\.]+$)'
    target_folder = '.'
    overwrite = False
    generate_slurm = False
    try:
        opts, args = getopt.getopt(argv, "hi:r:o:ws")
    except:
        print("Error in parsing inputs.")
        print("Usage: extract_fov_files.py -i <source_folder> -r <regular_expression_string> -o <target_folder>")
    for opt, arg in opts:
        if opt == '-h':
            print("Usage: extract_fov_files.py -i <source_folder> -r <regular_expression_string> -o <target_folder>")
            print(" -w: overwrite existing files")
            print(" -s: generate slurm instead of bash")
        elif opt in ['-i']:
            source_folder = arg
        elif opt in ['-r']:
            re_string = arg
        elif opt in ['-o']:
            target_folder = arg
        elif opt in ['-w']:
            overwrite = True
        elif opt in ['-s']:
            generate_slurm = True

    print(f"* Searching for files belong to different field-of-views. ")
    print(f"* source_folder: {source_folder}")
    print(f"* target_folder: {target_folder}")
    print(f"* searching string: {re_string}")
    print(f"* overwrite target files: {overwrite}")
    print(f"* generate slurm script: {generate_slurm}")
    return source_folder, target_folder, re_string, overwrite, generate_slurm

if __name__ == "__main__":
    # parse
    source_folder, target_folder, re_string, overwrite, generate_slurm = parse_arguments(sys.argv[1:])
    # trim folder filepath
    if source_folder[-1]  == os.path.sep:
        source_folder = source_folder[:-1]
    if target_folder[-1]  == os.path.sep:
        target_folder = target_folder[:-1]
    # create final target folder
    final_target_folder = os.path.join(target_folder, os.path.basename(source_folder))
    if not os.path.exists(final_target_folder):
        print(f'creating final target folder: {final_target_folder}')
        try:
            os.makedirs(final_target_folder,)
        except:
            # these are only required when having permission error
            old_umask = os.umask(000) 
            os.makedirs(final_target_folder,)
            os.umask(old_umask)
    
    fov_2_files = {'others':[]}
    # loop through folders
    for _parent_dir, _dirs, _files in os.walk(source_folder):
        #print(_parent_dir)
        for _file in _files:
            # search for regular expression match
            _match_result = re.match(re_string, _file)
            _rel_file = os.path.join(os.path.relpath(_parent_dir, source_folder), _file)
            if _match_result:
                if _match_result.groupdict()['fov'] not in fov_2_files:
                    fov_2_files[ _match_result.groupdict()['fov'] ] = [_rel_file]
                else:
                    fov_2_files[ _match_result.groupdict()['fov'] ].append(_rel_file)
            else:
                fov_2_files['others'].append(_rel_file)
    # save these temp list
    fov_2_savefile = {}
    for _fov, _files in fov_2_files.items():
        print(f"FOV: {_fov}, {len(_files)} files", end='')
        _fov_filelist_savefile = os.path.join(final_target_folder, f"filelist_{_fov}.txt")
        if not os.path.exists(_fov_filelist_savefile) or overwrite:
            with open(_fov_filelist_savefile, 'w') as _f:
                _f.write('\n'.join(_files))
            print(f", write to file: {_fov_filelist_savefile}")
        else:
            print("")
        fov_2_savefile[_fov] = _fov_filelist_savefile
    
    # print commands
    if generate_slurm:
        slurm_script_file = os.path.join(final_target_folder, 'fov_archiving.slurm')
        print(f"slurm script saved into file: {slurm_script_file}")
        if not os.path.exists(slurm_script_file) or overwrite:
            with open(slurm_script_file, 'w') as _sf:
                for _fov, _savefile in fov_2_savefile.items():
                    _sf.write(f'sbatch -p zhuang,shared -c 1 --mem 8000 -t 0-24:00 --wrap="time tar --use-compress-program zstd -C {source_folder} -T {_savefile} -cvf {final_target_folder+os.sep}Fov_{_fov}.tar.zst"\n')
                    _sf.write('sleep 1\n')
                _sf.write("echo Finish submitting fov based archiving jobs.\n")
    else:
        bash_script_file = os.path.join(final_target_folder, 'fov_archiving.bash')
        print(f"bash script saved into file: {bash_script_file}")
        if not os.path.exists(bash_script_file) or overwrite:
            with open(bash_script_file, 'w') as _sf:
                for _fov, _savefile in fov_2_savefile.items():
                    _sf.write(f"time tar --zstd -C {source_folder} -T {_savefile} -cvf {final_target_folder+os.sep}Fov_{_fov}.tar.zst\n")