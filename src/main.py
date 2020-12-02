
def main():
    from numpy.random import Generator, PCG64
    rg = Generator(PCG64())
    rg.standard_normal()


if __name__ == "__main__":
    main()
