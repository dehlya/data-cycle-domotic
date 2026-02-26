# Async sensor polling — fetches Jimmy & Jérémie JSON endpoints concurrently, writes raw files to Bronze


from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

#######################
### SMB Apartments .json file

class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            print(f"New file detected : {event.src_path}")
            time.sleep(1) 
            # Copie avec gestion d'erreur
    

# UNC SMB
path = r""


observer = Observer()
observer.schedule(NewFileHandler(), path=path, recursive=False)
observer.start()

try:
    while True:
	# Run in background, main thread can do other things or sleep
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
