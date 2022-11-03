#!~/anaconda3/bin/python

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
        print("Usage: extract_fov_files.py -i <source_folder> -r <regular_expression_string> -o <target_folder> [-h|-w|-s]")
    for opt, arg in opts:
        if opt == '-h':
            print("Usage: extract_fov_files.py -i <source_folder> -r <regular_expression_string> -o <target_folder>  [-h|-w|-s]")
            print(" -w: overwrite existing files")
            print(" -s: generate slurm instead of bash")
            exit()
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
    print(f"* Archiving data by field-of-view")
    print(f"-- by Pu Zheng, 2022.10.19")
    print(f"* Searching for files belong to different field-of-views. ")
    print(f"-- source_folder: {source_folder}")
    print(f"-- target_folder: {target_folder}")
    print(f"-- searching string: {re_string}")
    print(f"-- overwrite target files: {overwrite}")
    print(f"-- generate slurm script: {generate_slurm}")
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
    if len(fov_2_files['others']) == 0:
        del fov_2_files['others']
    # save these temp list
    fov_2_filelist_savefile = {}
    fov_2_archive_savefile = {}
    fov_2_log_savefile = {} 
    for _fov, _files in sorted(fov_2_files.items()):
        print(f"FOV: {_fov}, {len(_files)} files", end=',')
        _fov_filelist_savefile = os.path.join(final_target_folder, f"filelist_{_fov}.txt")
        _fov_archive_savefile = os.path.join(final_target_folder, f"Fov_{_fov}.tar.zst")
        _fov_log_savefile = os.path.join(final_target_folder, f"Fov_{_fov}.log")
        # if filelsit doesn't exist, create.
        if not os.path.exists(_fov_filelist_savefile) or overwrite:
            with open(_fov_filelist_savefile, 'w', encoding='utf-8') as _f:
                _f.write('\n'.join(_files))
            print(f"write filelist to: {_fov_filelist_savefile}", end=';')
        else:
            print("filelist exists", end=';')
        # if archive doesn't exist, create and add to processing list.
        if not os.path.exists(_fov_archive_savefile) or overwrite:
            print(f"archive to: {_fov_archive_savefile}", end=';')
            # append
            fov_2_filelist_savefile[_fov] = _fov_filelist_savefile
            fov_2_archive_savefile[_fov] = _fov_archive_savefile
        else:
            print("archive exists", end=';')
        # if log doesn't exist, append this for logging job
        if not os.path.exists(_fov_log_savefile) or overwrite:
            print(f"write log to: {_fov_log_savefile}", end=';')
            # append
            fov_2_log_savefile[_fov] = _fov_log_savefile
        else:
            print("log exists", end=';')
        print("")
    print(f"{len(fov_2_files)} field of views detected. ")
    # print commands
    if generate_slurm:
        # archiving
        archiving_slurm_script_file = os.path.join(final_target_folder, 'fov_archiving.slurm')
        if not os.path.exists(archiving_slurm_script_file) or overwrite:
            print(f"Archiving slurm script saved into file: {archiving_slurm_script_file}")
            with open(archiving_slurm_script_file, 'w', encoding='utf-8') as _sf:
                _sf.write("#!/bin/bash\n")
                _sf.write(r"#SBATCH -e ./Logs/slurm-%j.err"+'\n')
                _sf.write(r"#SBATCH -o ./Logs/slurm-%j.out"+'\n')
                for _fov in fov_2_archive_savefile:
                    _filelist_savefile = fov_2_filelist_savefile[_fov]
                    _archive_savefile = fov_2_archive_savefile[_fov]
                    # write line
                    _sf.write(f'sbatch -p zhuang,shared -c 1 --mem 4000 -t 0-6:00 --wrap="time tar --use-compress-program zstd -C {source_folder} -T {_filelist_savefile} -cvf {_archive_savefile}" -o ./Logs/Archiving_logs/{os.path.basename(source_folder)}_archive_fov_{_fov}.log\n')
                    _sf.write('sleep 1\n')
                _sf.write("echo Finish submitting fov based archiving jobs.\n")
        # scanning archives
        scanning_slurm_script_file = os.path.join(final_target_folder, 'fov_scanning.slurm')
        if not os.path.exists(scanning_slurm_script_file) or overwrite:
            print(f"Scanning slurm script saved into file: {scanning_slurm_script_file}")
            with open(scanning_slurm_script_file, 'w', encoding='utf-8') as _sf:
                _sf.write("#!/bin/bash\n")
                _sf.write(r"#SBATCH -e ./Logs/slurm-%j.err"+'\n')
                _sf.write(r"#SBATCH -o ./Logs/slurm-%j.out"+'\n')
                for _fov, _log_savefile in fov_2_log_savefile.items():
                    _archive_savefile = _log_savefile.replace('.log', '.tar.zst')
                    _sf.write(f'sbatch -p zhuang,shared -c 1 --mem 4000 -t 0-6:00 --wrap="time tar --use-compress-program=unzstd -tf {_archive_savefile} > {_log_savefile}" -o ./Logs/Scanning_logs/{os.path.basename(source_folder)}_scan_fov_{_fov}.log\n')
                    _sf.write('sleep 1\n')
                _sf.write("echo Finish submitting fov based scanning jobs.\n")
        # clean up file-lists and logs
        cleanning_slurm_script_file = os.path.join(final_target_folder, 'fov_cleaning.slurm')
        if not os.path.exists(cleanning_slurm_script_file) or overwrite:
            print(f"Cleaning up slurm script saved into file: {cleanning_slurm_script_file}")
            with open(cleanning_slurm_script_file, 'w', encoding='utf-8') as _sf:
                _sf.write("#!/bin/bash\n")
                _sf.write(r"#SBATCH -e ./Logs/slurm-%j.err"+'\n')
                _sf.write(r"#SBATCH -o ./Logs/slurm-%j.out"+'\n')
                # archiving filelists
                _sf.write(f"tar -cvf {os.path.join(final_target_folder, r'filelists.tar')} {os.path.join(final_target_folder, r'filelist_*.txt')}\n")
                # archiving logs
                _sf.write(f"tar -cvf {os.path.join(final_target_folder, r'logs.tar')} {os.path.join(final_target_folder, r'Fov_*.log')}\n")
                # clean up
                _sf.write(f"rm {os.path.join(final_target_folder, r'filelist_*.txt')}\n")
                _sf.write(f"rm {os.path.join(final_target_folder, r'Fov_*.log')}\n")

        # checking results
        # please run the next python script
        # print instructions:
        print(f'Please run the following code:')
        print(f"1. archving data:")
        print(f'sbatch {archiving_slurm_script_file}')
        print(f"2. scanning data archives:")
        print(f'sbatch {scanning_slurm_script_file}')
        print(f"3. scanning data archives:")
        print(f'sbatch --wrap="python check_archives.py -o {final_target_folder} -w" -o {os.path.basename(source_folder)}_archive_summary.txt')
        print(f"4. archive filelists and logs")
        print(f"python check_archives.py -o {final_target_folder} -w")
        print(f"-- check the final output! ")

    else:
        # archiving
        archiving_bash_script_file = os.path.join(final_target_folder, 'fov_archiving.bash')
        if not os.path.exists(archiving_bash_script_file) or overwrite:
            print(f"Archiving bash script saved into file: {archiving_bash_script_file}")
            with open(archiving_bash_script_file, 'w', encoding='utf-8') as _sf:
                _sf.write("#!/bin/bash\n")
                for _fov in fov_2_archive_savefile:
                    _filelist_savefile = fov_2_filelist_savefile[_fov]
                    _archive_savefile = fov_2_archive_savefile[_fov]
                    # write line
                    #_sf.write(f"time tar --zstd -C {source_folder} -T {_savefile} -cvf {final_target_folder+os.sep}Fov_{_fov}.tar.zst\n")
                    _sf.write(f"time tar --use-compress-program zstd -C {source_folder} -T {_filelist_savefile} -cvf {_archive_savefile}\n")
        # scanning archives
        scanning_bash_script_file = os.path.join(final_target_folder, 'fov_scanning.bash')
        if not os.path.exists(scanning_bash_script_file) or overwrite:
            print(f"Scanning bash script saved into file: {scanning_bash_script_file}")
            with open(scanning_bash_script_file, 'w', encoding='utf-8') as _sf:
                _sf.write("#!/bin/bash\n")

                for _fov, _log_savefile in fov_2_log_savefile.items():
                    _archive_savefile = _log_savefile.replace('.log', '.tar.zst')
                    _sf.write(f"echo scanning archive: {_archive_savefile}\n")
                    _sf.write(f"time tar --use-compress-program=unzstd -tf {_archive_savefile} > {_log_savefile}\n")

        # checking results
        # please run the next python script
        # print instructions:
        print(f'Please run the following code:')
        print(f"1. archving data:")
        print(f'bash {archiving_bash_script_file}')
        print(f"2. scanning data archives:")
        print(f'bash {scanning_bash_script_file}')
        print(f"3. scanning data archives:")
        print(f"python check_archives.py -o {final_target_folder} -w")
        print(f"-- check the final output! ")