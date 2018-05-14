# Building Transporter on Windows

1) Install Go

First step, download and install Go. Go to [https://golang.org/dl/](https://golang.org/dl/) and install the current featured Go for Microsoft Windows. At time of writing, this is `go1.10.2.windows-amd64.msi`. Run the downloaded MSI installer.

2) Install Git

Go to [https://git-for-windows.github.io/](https://git-for-windows.github.io/) and download the Windows version. Install that. It's a good idea to select to option to put the Git Bash shell either on your desktop or somewhere equally accessible.

3) Make directories

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

4) Clone transporter

We now can download the transporter source into this directory

```
git clone https://github.com/compose/transporter
```

5) Build the transporter

Finally, we can issue the build command

```
cd transporter
go get -a ./...
go build ./cmd/transporter
```

and that should give us ```transporter.exe``` in the current directory.
