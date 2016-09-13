COMPILER_CACHE:=ccache

# If you are only doing local development, you don't need to build all
# of the possible variants of Tensorflow.  In that case, you can set
# one of the following depending upon the capability of your computer:
#
# If `cat /proc/cpuinfo | grep avx2` returns something
# TF_KERNEL_VARIANTS:=avx2
# Otherwise, if `cat /proc/cpuinfo | grep avx` returns something
# TF_KERNEL_VARIANTS:=avx
# Otherwise, any machine you're likely to have will do fine with this
# TF_KERNEL_VARIANTS:=sse42
# And if Tensorflow crashes with an illegal instruction, this is the fallback
# TF_KERNEL_VARIANTS:=sse2

# You should run ccache -M 20G or something similar to set the max cache size
# to a value higher than the default 1G.
