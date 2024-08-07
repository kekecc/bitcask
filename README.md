##  bitcask
A simple but fast key-value storage base on [Bitcask](https://riak.com/assets/bitcask-intro.pdf) model.

## Basic Architecture
###  storage format in memory index
![avatar](./pngs/0ECF81C2A704C499BC6926D2E64EDBE8.png)
### storage format in disk
![avatar](./pngs/5E6A51CDDF16AA232E75F69A9C1222CE.png)
![avatar](./pngs/205CCD876A5972DA88D3A735EB7C8F41.png)
### read path
![avatar](./pngs/20B74B3A8A484840DFCF60192DFB50A8.png)
### merge process
![avatar](./pngs/D38693796BC26BFB3FC1167D717F8CB1.png)