# Environment
使用了Conda
且为了加速使用了cuDF，不过cuDF的安装只能使用WSL，而不能在Windows环境下直接安装

在WSL下启动conda
```
source ~/miniconda3/bin/activate
```

conda create --name short_term_trade --file environment.txt
conda create -n short_term_trade -c rapidsai -c conda-forge -c nvidia  \
    rapids=25.02 python=3.11 'cuda-version>=12.0,<=12.8' \
    jupyterlab


# https://docs.rapids.ai/install/#docker
docker run --gpus all nvcr.io/nvidia/k8s/cuda-sample:nbody nbody -gpu -benchmark
Docker CE v18 & nvidia-docker2 users will need to replace the following for compatibility: docker run --gpus all with docker run --runtime=nvidia


nvidia-docker run --runtime=nvidia nvcr.io/nvidia/k8s/cuda-sample:nbody nbody -gpu -benchmark

docker run \
    --rm \
    -it \
    --pull always \
    --gpus all \
    --shm-size=1g --ulimit memlock=-1 --ulimit stack=67108864 \
    -v $(pwd)/environment.yml:/home/rapids/environment.yml \
    rapidsai/base:25.02-cuda12.8-py3.12

docker run --gpus all -it rapidsai/rapidsai:23.08a-cuda12.0.1-py3.9 bash
docker run --gpus all -it rapidsai/base:25.02-cuda12.8-py3.12 bash
rapidsai/base:25.04a-cuda12.8-py3.11
docker run --gpus all -it rapidsai/base:25.04a-cuda12.8-py3.11 bash
{
    "runtimes": {
        "nvidia": {
            "args": [],
            "path": "nvidia-container-runtime"
        }
    }
}
