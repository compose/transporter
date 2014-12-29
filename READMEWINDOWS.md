## Building Transporter on Windows

1) Install Go

First step, download and install Go. Go to [https://golang.org/dl/](https://golang.org/dl/) and install the `go1.4.windows-amd64.msi ` file offered.

2) Install Git

Go supports multiple source code control systems and you need to get the tools to handle them. Refer to [https://code.google.com/p/go-wiki/wiki/GoGetTools]( https://code.google.com/p/go-wiki/wiki/GoGetTools)  for the canonical current list. 

First and foremost, we need Git...

Go to [http://git-scm.com/downloads](http://git-scm.com/downloads) and download the Windows version. Install that. It's a good idea to select to option to put the Git Bash shell either on your desktop or somewhere equally accessible.

3) Install Mercurial

The other source code control tool you'll need for this build is Mercurial, so go to [http://mercurial.selenic.com/wiki/Download](http://mercurial.selenic.com/wiki/Download) and download `Mercurial-3.2.3 (64-bit msi)`.

4) Make directories

Start the Git Bash shell. We now need to make the go build hierachy:

```
mkdir go
```

We need to set the GOPATH environment variable. This is going to point at the directory we just made and lets Go locate other packages. We'll just set it temporarily here like so:

```
export GOPATH=`pwd`/go/
```

Nowe we can build the rest of the tree:

```
cd go
mkdir pkg bin src
cd src
mkdir github.com
cd github.com
mkdir compose
cd compose
```

5) Clone transporter

We now can download the transporter source into this directory

```
git clone https://github.com/compose/transporter
```

6) Get the dependencies

And with the source downloaded, we can go into the directory and ask it to download all the libraries it depends upon.

```
cd transporter
go get -a ./cmd/transporter
```

7) Build the transporter

Finally, we can issue the build command

```
go build -a ./cmd/transporter
```

and that should give us ```transporter.exe``` in the current directory.
