// Copyright 2021 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package system

import (
	"context"

	"code.gitea.io/gitea/models/db"
)

// AppState represents a state record in database
// if one day we would make Gitea run as a cluster,
// we can introduce a new field `Scope` here to store different states for different nodes
type AppState struct {
	ID       string `xorm:"pk varchar"`
	Revision int64
	Content  string `xorm:"VARCHAR"`
}

func init() {
	db.RegisterModel(new(AppState))
}

// SaveAppStateContent saves the app state item to database
func SaveAppStateContent(key, content string) error {
	return db.WithTx(db.DefaultContext, func(ctx context.Context) error {
		eng := db.GetEngine(ctx)
		// try to update existing row
		cnt, err := eng.Table("app_state").Where("id = ?", key).Count()
		if err != nil {
			return err
		}
		if cnt == 0 {
			// if no existing row, insert a new row
			_, err = eng.Insert(&AppState{ID: key, Content: content})
			return err
		} else {
			_, err := eng.Exec("UPDATE app_state SET revision=revision+1, content=? WHERE id=?", content, key)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// GetAppStateContent gets an app state from database
func GetAppStateContent(key string) (content string, err error) {
	e := db.GetEngine(db.DefaultContext)
	appState := &AppState{ID: key}
	has, err := e.Get(appState)
	if err != nil {
		return "", err
	} else if !has {
		return "", nil
	}
	return appState.Content, nil
}
