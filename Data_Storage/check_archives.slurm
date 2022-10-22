#!/bin/bash
##SBATCH -n 1
##SBATCH -N 1
##SBATCH -p zhuang
##SBATCH -t 2-00:00:00
##SBATCH --mem 8000
##SBATCH --open-mode=append
##SBATCH -o /n/holyscratch01/zhuang_lab/Users/pzheng/Analysis_results/Logs/job_stdoe_tmp/%j.stdout.txt
##SBATCH -e /n/holyscratch01/zhuang_lab/Users/pzheng/Analysis_results/Logs/job_stdoe_tmp/%j.stdout.txt
# loop through all folders in current directory 
for FILE in $1/*.tar.zst; do 
	echo Submitting "$FILE" # Start
	sbatch -p zhuang,shared -c 4 --mem 8000 -t 0-36:00 --wrap="time tar --use-compress-program=unzstd -tf ${FILE} > ${FILE}_test.log" # run check and save into a test.log file
	sleep 1 # pause to be kind to scheduler
done
echo Finish submitting checking jobs
# this will generate a slurm-xxxxxx.out file for this job and each submitted job




