mkdir -p cmake/build
rm cmake/build/config
cp server/config cmake/build/config
pushd cmake/build
cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../..
make -j 4