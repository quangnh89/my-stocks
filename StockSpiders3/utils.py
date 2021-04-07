from datetime import datetime
import json


class Config:
    def __init__(self):
        with open('do_not_delete.json') as f:
            self.file = f
            self.config = json.load(f)
            print('Self Config', self.config)
            self.last_run_minute = self.config['minute']
            self.last_run_hour = self.config['hour']
            self.last_run_day = self.config['day']

    def f_last_run_minute(self):
        return self.last_run_minute

    def f_last_run_hour(self):
        return self.last_run_hour

    def f_last_run_day(self):
        return self.last_run_day

    def f_update_last_run(self, resolution='D'):
        now = int(datetime.now().timestamp())
        if resolution == 'D':
            self.config['day'] = now
        elif resolution == '1':
            self.config['minute'] = now
        elif resolution == '60':
            self.config['hour'] = now
        else:
            pass
        with open("do_not_delete.json", "w") as f:
            json.dump(self.config, f)

    def __del__(self):
        self.file.close()
