from setuptools import setup, find_packages


setup(name='pylogrouter',
      version='0.0.0',
      author='Matthew Via',
      author_email='via@matthewvia.info',
      description='Log collection and distribution daemon.',
      license='BSD-2',
      entry_points="""
          [console_scripts]
          pylogrouter=pylogrouter.pylogrouter:main
          [pylogrouter.plugin]
          HTTPSink=pylogrouter.builtin:HTTPSink
          HTTPSource=pylogrouter.builtin:HTTPSource
          FlumeHTTPSink=pylogrouter.builtin:FlumeHTTPSink
          MemoryPipe=pylogrouter.builtin:MemoryPipe
          SyslogSource=pylogrouter.builtin:SyslogSource
          PrinterSink=pylogrouter.builtin:PrinterSink
          StaticHeader=pylogrouter.builtin:StaticHeader
          """,
      packages=find_packages())

