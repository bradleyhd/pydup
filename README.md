# pydup

Pydup finds and removes duplicate files across multiple media, regardless of whether all media are simultaneously accessible. If you're the one who has several external HDDs with duplicate files, this is for you.

To use, you'll need:
- Python 3
- MongoDB

## How to use Pydup
1. Clone the repository locally.
2. Mount your target volume.
3. Run `python3 scan.py /path/to/root/of/volume VolumeNickname` where VolumeNickname is a string that identifies the volume. Options for ignoring small files and batch inserting into MongoDB are available via `python3 scan.py -h`. You may unmount the volume when this completes.
4. Repeat step 3 for each of your volumes, changing the nickname each time.
5. Run `python3 review.py`, which will produce `review_me.csv`. Edit the CSV and remove any lines that correspond to files you wish to keep.
6. Run `python3 mark.py` to update MongoDB and mark file records for deletion.
7. Mount a target volume.
8. Run `python3 cleanup.py VolumeNickname` to remove the files marked for deletion.
9. Repeat step 8 for each of your volumes.
10. Congrats! You're rid of all those unmanageable duplicate files.

## Caveats
- Full paths to files are stored in MongoDB, so please do not rename a volume or use a different mount point when re-mounting a drive for file deletion.
- Duplicate files are currently determined by simply taking the MD5 of the filename and filesize. File content signatures are on the to-do list. 

## Issues?
Please feel free to raise issues or submit pull requests.
