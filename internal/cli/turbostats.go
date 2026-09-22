package cli

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/turbostats/wire"
)

func newTurbostatsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "turbostats",
		Short: "Work with the TurboStats reporter's credentials",
	}
	cmd.AddCommand(newKeygenCommand())
	return cmd
}

func newKeygenCommand() *cobra.Command {
	var out string
	cmd := &cobra.Command{
		Use:   "keygen",
		Short: "Generate a TurboStats keypair: the private half to a file, the public half to register",
		Long: "Generate a TurboStats keypair on this machine.\n\n" +
			"The credential, the private half, is written to --out, readable only by\n" +
			"its owner, and never printed. The public key is printed: send it to\n" +
			"whoever runs your control plane to register this instance. It is safe to\n" +
			"share. The key id is printed too; the control plane files the key under\n" +
			"the same id.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return keygen(out, rand.Reader, cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "File to write the credential to; it must not exist")
	cmd.MarkFlagRequired("out")
	return cmd
}

// keygen generates a keypair from random, writes the credential to out, and
// prints the public key and key id to w.
//
// The credential goes to a file and nowhere else. Standard output ends up
// in terminal scrollback and CI logs, and a private key there is a leaked
// key. The file is created, never truncated: one that exists is someone's
// key, and replacing it would strand every instance that uses it.
func keygen(out string, random io.Reader, w io.Writer) error {
	pub, priv, err := ed25519.GenerateKey(random)
	if err != nil {
		return fmt.Errorf("generating a key: %w", err)
	}
	credential, err := wire.FormatCredential(priv.Seed())
	if err != nil {
		return err
	}
	public, err := wire.FormatPublicKey(pub)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("%s exists; keygen does not overwrite a key", out)
	}
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(f, credential); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	fmt.Fprintf(w, "key id:     %s\n", wire.KeyID(pub))
	fmt.Fprintf(w, "public key: %s\n", public)
	fmt.Fprintf(w, "\nSend the public key to your control plane to register this instance.\n"+
		"The credential is in %s. Put its contents in SQLFLOW_TURBOSTATS_KEY,\n"+
		"and send it to nobody.\n", out)
	return nil
}
