import glob
import json
import os
import sys
import tkinter as tk
from tkinter import filedialog
import numpy as np
from tkinter import messagebox

class TK_GUI():
    def search_file(self):
        self.initaldir = 'C:\\Python_DWH\\Json_Files'
        self.path_to_read = filedialog.askdirectory(initialdir=self.initaldir)
        search = sorted(glob.glob(os.path.join(self.path_to_read, "*.json")))
        for i, file in enumerate(search):
            if os.path.basename(file) == 'QUERY.json':
                with open(file, 'r', encoding='utf-8') as j:
                    self.query_json = json.loads(j.read())

        if not hasattr(self,"query_json"):
            messagebox.showerror("Error","No query_json file found")
        else:
            self.read_json()

    def read_json(self):
        self.window1 = tk.Toplevel(self.window)
        self.window1.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.window.withdraw()
        self.all_queries = []
        for query_name in self.query_json.keys():
            for query_type in self.query_json[query_name]:
                self.all_queries.append(f"{query_name}\n{query_type}")
        self.window1.rowconfigure(0, weight=1)
        for i, button_text in enumerate(self.all_queries):
            self.window1.columnconfigure(i, weight=1)
            button = tk.Button(self.window1, text=f"{button_text}",
                               command=lambda text=button_text: self.get_button(text))
            button.grid(row=0, column=i, padx=5, pady=5)

        self.window1.geometry("1000x200")


    def on_closing(self):
        self.window1.destroy()
        self.window.destroy()
    def get_button(self,name):
        query_name,query_type = name.split("\n")
        with open(f"C:\\Python_DWH\Bat\\Queries\\{query_name}_{query_type}.txt","w") as f:
            f.write(self.query_json[query_name][query_type])

        messagebox.showinfo("Successful", f"Created File {f"C:\\Python_DWH\Bat\\Queries\\{query_name}_{query_type}.txt"}")

    def __init__(self):
        self.window = tk.Tk()
        self.window.title('Query Selection')
        select_button = tk.Button(self.window,text="Select Folder", command=self.search_file)
        select_button.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        self.window.geometry("300x200")
        self.window.mainloop()

if __name__ == "__main__":
    tk = TK_GUI()