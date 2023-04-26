import os


class WriteFile():
    def check_folder_exist(self):
        if not os.path.isdir('check_result'):
            os.mkdir('check_result')
