package main

import (
	"errors"
	"fmt"

	"github.com/urfave/cli/v2"
	"golang.org/x/crypto/bcrypt"
)

func runHashPw(c *cli.Context) error {
	pw := c.Args().First()
	if pw == "" {
		return errors.New("usage: connqc hashpw <password>")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	fmt.Println(string(hash))
	return nil
}
