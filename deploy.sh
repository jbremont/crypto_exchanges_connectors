
rm -r dist
python setup.py bdist_wheel

for file in ./dist/*.*; do
  curl -F "package=@$file" https://push.fury.io/qyR-84-5JSEx7Nw1xN7N
done
