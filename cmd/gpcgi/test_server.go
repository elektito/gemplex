package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"time"

	"github.com/elektito/gemplex/pkg/config"
	"github.com/elektito/gemplex/pkg/utils"
)

func pubKey(priv interface{}) interface{} {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &k.PublicKey
	case *ecdsa.PrivateKey:
		return &k.PublicKey
	default:
		return nil
	}
}

func generateCert() tls.Certificate {
	priv, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}

	templ := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Hour * 24 * 180),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &templ, &templ, pubKey(priv), priv)
	utils.PanicOnErr(err)

	out := &bytes.Buffer{}
	err = pem.Encode(out, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	utils.PanicOnErr(err)
	certPemBytes := []byte(out.String())
	out.Reset()

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	utils.PanicOnErr(err)
	err = pem.Encode(out, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
	utils.PanicOnErr(err)
	keyPemBytes := []byte(out.String())

	cert, err := tls.X509KeyPair(certPemBytes, keyPemBytes)
	utils.PanicOnErr(err)

	return cert
}

func testServe(cfg *config.Config) {
	// generate a throw-away self-signed certificate
	cert := generateCert()

	tlsCfg := tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	addr := "127.0.0.1:1965"
	listener, err := tls.Listen("tcp", addr, &tlsCfg)
	utils.PanicOnErr(err)

	log.Println("Listening on:", addr)
	for {
		conn, err := listener.Accept()
		utils.PanicOnErr(err)

		go handleConn(conn, cfg)
	}
}

func handleConn(conn net.Conn, cfg *config.Config) {
	defer conn.Close()

	log.Println("Accepted connection from:", conn.RemoteAddr())
	params := Params{
		SearchDaemonSocket: cfg.Search.UnixSocketPath,
		ServerName:         "localhost",
	}
	cgi(conn, conn, params)
}
