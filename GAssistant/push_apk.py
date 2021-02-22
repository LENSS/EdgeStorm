import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--push_model", help="push the model initialization files needed", default=False, type = bool)

args = parser.parse_args()

device_list = ["PM1LHMA7C0605571", "PM1LHMA851700906", "PM1LHMA851702725"]
for device in device_list:
    print("Pushing to " + device)
    if args.push_model:
        cmd = ""
        cmd = "adb -s " + device + " push /home/liuyi/Desktop/files/ /storage/emulated/0/Android/data/"
        os.system(cmd)
    cmd = ""
    cmd = "adb -s " + device + " push app/build/outputs/apk/debug/app-debug.apk storage/emulated/0/distressnet/MStorm/APK/GAssistant.apk"
    os.system(cmd)
print("pushed successful")
