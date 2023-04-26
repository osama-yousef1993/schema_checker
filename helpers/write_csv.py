import pandas as pd


class WriteCSV():

    def build_csv(self, file_name,  data):
        df = pd.DataFrame(data).apply(pd.Series)
        df.style.applymap(self.color_negative_red, subset=['Matching'])
        df.to_csv(f'check_result/{file_name}.csv', index=True)

    def color_negative_red(val):
        color = 'red' if val else 'green'
        return 'color: %s' % color
