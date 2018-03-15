import subprocess

def runCmd(cmd):
  spl = cmd.split()
  output = subprocess.check_output(spl)
  ret = []
  for line in output.split():
    ret.append(line.strip().decode('utf-8'))
  return ret